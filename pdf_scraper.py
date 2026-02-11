# src/tools/pdf_scraper.py

import re
import pdfplumber
import hashlib
import fitz  # PyMuPDF
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.dialects.sqlite import insert  # For upsert on SQLite
from src.database.db import get_session
from src.database.models import Run, TextNode
from datetime import datetime
import os
import logging
from collections import Counter, defaultdict

# Configure logging at module level
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

class PDFScraper:
    def __init__(self, pdf_path):
        self.pdf_path = os.path.abspath(pdf_path)
        self.results = []
        self.run_id = None
        self.pdf_doc = None
        self.boilerplate = set()

    def generate_id(self, text):
        return hashlib.md5(text.encode('utf-8')).hexdigest()[:10]

    def build_boilerplate_map(self, sample_limit=15):
        """Identifies text strings that appear at the same vertical position (headers/footers)."""
        y_text_map = Counter()
        with pdfplumber.open(self.pdf_path) as pdf:
            sample_pages = pdf.pages[:sample_limit]
            for page in sample_pages:
                words = page.extract_words()
                for w in words:
                    key = (w['text'].strip(), round(w['top'], 0))
                    y_text_map[key] += 1
                
        threshold = len(sample_pages) * 0.4
        self.boilerplate = {key for key, count in y_text_map.items() if count >= threshold}
        logger.info(f"Mapped {len(self.boilerplate)} boilerplate elements to ignore.")

    def is_rule_marker(self, text):
        patterns = {
            'part': r'^Part\s+\d+\.\d+',
            'main': r'^\d+\.\d+\.\d+',
            'digit': r'^\(\d+\)',
            'alpha': r'^\([a-z]\)',
            'roman': r'^\([ivx]+\)'
        }
        for level, pat in patterns.items():
            if re.match(pat, text):
                return level, pat
        return None, None

    def _ensure_storage_dirs(self, run_id, node_uid):
        storage_root = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'storage')
        pdf_dir = os.path.join(storage_root, 'pdfs', str(run_id))
        os.makedirs(pdf_dir, exist_ok=True)
        return os.path.join(pdf_dir, f"{node_uid}.pdf")

    def _generate_pdf_excerpt(self, page_num_0idx, block_bbox, node_uid):
        """Generates a full-width PDF crop with a yellow highlight over the extracted text."""
        PADDING_VERT = 80
        
        # Source page dimensions
        src_page = self.pdf_doc[page_num_0idx]
        page_width = src_page.rect.width
        page_height = src_page.rect.height

        x0, top, x1, bottom = block_bbox

        # Define crop area: Full width, padded vertically
        crop_top = max(0, top - PADDING_VERT)
        crop_bottom = min(page_height, bottom + PADDING_VERT)
        crop_x0 = 0
        crop_x1 = page_width

        rect = fitz.Rect(crop_x0, crop_top, crop_x1, crop_bottom)

        # Create new document and page
        new_doc = fitz.open()
        new_page = new_doc.new_page(width=rect.width, height=rect.height)
        
        # Place original content onto the new page
        new_page.show_pdf_page(new_page.rect, self.pdf_doc, page_num_0idx, clip=rect)

        # Add Highlight: Calculate coordinates relative to the new cropped page
        # Note: Since crop_x0 is 0, highlight_x0 remains the same as source x0
        highlight_rect = fitz.Rect(
            x0,                 # x0
            top - crop_top,      # y0 (relative to crop)
            x1,                 # x1
            bottom - crop_top    # y1 (relative to crop)
        )
        
        # Draw semi-transparent yellow rectangle
        new_page.draw_rect(
            highlight_rect, 
            color=None, 
            fill=(1, 1, 0), 
            fill_opacity=0.3
        )

        output_path = self._ensure_storage_dirs(self.run_id, node_uid)
        new_doc.save(output_path)
        new_doc.close()

    def scrape(self):
        logger.info("=== Starting PDF Scraper ===")
        logger.info(f"Input PDF: {self.pdf_path}")

        if not os.path.exists(self.pdf_path):
            logger.error(f"PDF file not found: {self.pdf_path}")
            raise FileNotFoundError(self.pdf_path)

        # Build boilerplate map first
        self.build_boilerplate_map()

        # Create Run record
        with get_session() as session:
            new_run = Run(pdf_path=self.pdf_path, timestamp=datetime.utcnow(), status='processing')
            session.add(new_run)
            session.commit()
            self.run_id = new_run.id
            session.refresh(new_run)

        logger.info(f"Created Run ID: {self.run_id}")
        self.pdf_doc = fitz.open(self.pdf_path)

        # State for block building
        state = {'part': '', 'main': '', 'digit': '', 'alpha': '', 'roman': ''}
        buffer = {
            'text_parts': [],
            'bbox': None, # [x0, top, x1, bottom]
            'page': None,
            'rule_code': "",
            'style': {'size': 0, 'bold': False, 'italic': False}
        }

        def flush_buffer():
            if not buffer['text_parts']:
                return
            
            full_text = " ".join(buffer['text_parts']).strip()
            full_text = re.sub(r'\s+', ' ', full_text)
            if not full_text: return

            uid = self.generate_id(full_text)
            node_data = {
                'uid': uid,
                'run_id': self.run_id,
                'page': buffer['page'],
                'x_indent': round(buffer['bbox'][0], 0),
                'text': full_text,
                'rule_code': buffer['rule_code'],
                'font_size': buffer['style']['size'],
                'is_bold': buffer['style']['bold'],
                'is_italic': buffer['style']['italic'],
                'type': 'RULE' if buffer['rule_code'] else 'TEXT',
                'status': 'unverified'
            }
            self.results.append(node_data)

            try:
                self._generate_pdf_excerpt(buffer['page'] - 1, buffer['bbox'], uid)
            except Exception as e:
                logger.warning(f"Failed to generate excerpt for UID {uid}: {e}")
            
            # Reset buffer
            buffer['text_parts'] = []
            buffer['bbox'] = None

        with pdfplumber.open(self.pdf_path) as pdf:
            total_pages = len(pdf.pages)
            for page_num_1idx, page in enumerate(pdf.pages, start=1):
                h = page.height
                lines = page.extract_text_lines(layout=True, strip=True)

                for line in lines:
                    text = line['text'].strip()
                    if not text: continue

                    # Boilerplate and Margin Filtering
                    if (text, round(line['top'], 0)) in self.boilerplate: continue
                    if line['top'] < (h * 0.05) or line['bottom'] > (h * 0.93): continue

                    marker_level, marker_pat = self.is_rule_marker(text)
                    
                    # Logic: Determine if we should start a new block
                    is_note = text.lower().startswith("note:")
                    is_new_sentence_block = text[0].isupper() and (
                        not buffer['text_parts'] or 
                        buffer['text_parts'][-1].endswith(('.', ';', ':'))
                    )

                    if marker_level or is_note or is_new_sentence_block:
                        flush_buffer()
                        
                        buffer['page'] = page_num_1idx
                        first_char = line['chars'][0] if line['chars'] else {}
                        buffer['style'] = {
                            'size': round(first_char.get('size', 0), 1),
                            'bold': 'bold' in first_char.get('fontname', '').lower(),
                            'italic': 'italic' in first_char.get('fontname', '').lower(),
                        }

                        if marker_level:
                            match = re.match(marker_pat, text)
                            m_val = match.group(0)
                            levels = ['part', 'main', 'digit', 'alpha', 'roman']
                            start_idx = levels.index(marker_level)
                            for l in levels[start_idx:]: state[l] = ""
                            state[marker_level] = m_val
                            
                            code = state['main'] + state['digit'] + state['alpha'] + state['roman']
                            buffer['rule_code'] = code if code else state['part']
                            text = text[match.end():].strip() # Remove marker from text start
                        else:
                            buffer['rule_code'] = ""

                    # Accumulate text and expand bounding box
                    buffer['text_parts'].append(text)
                    l_bbox = [line['x0'], line['top'], line['x1'], line['bottom']]
                    if buffer['bbox'] is None:
                        buffer['bbox'] = l_bbox
                    else:
                        buffer['bbox'] = [
                            min(buffer['bbox'][0], l_bbox[0]),
                            min(buffer['bbox'][1], l_bbox[1]),
                            max(buffer['bbox'][2], l_bbox[2]),
                            max(buffer['bbox'][3], l_bbox[3])
                        ]

            flush_buffer()

        if not self.results:
            logger.warning("No text blocks extracted.")
            with get_session() as session:
                run = session.query(Run).get(self.run_id)
                run.status = 'completed_no_text'
                session.commit()
            self.pdf_doc.close()
            return self.run_id

        # Insert into DB
        logger.info(f"Inserting {len(self.results)} TextNode blocks into database...")
        with get_session() as session:
            stmt = insert(TextNode).values(self.results)
            stmt = stmt.on_conflict_do_nothing(index_elements=['uid'])
            session.execute(stmt)
            session.commit()

        self._assign_parents()

        with get_session() as session:
            run = session.query(Run).get(self.run_id)
            run.status = 'completed'
            session.commit()

        logger.info(f"=== PDF Scraping Completed: Run ID {self.run_id} ===")
        self.pdf_doc.close()
        return self.run_id

    def _assign_parents(self):
        with get_session() as session:
            nodes = session.query(TextNode)\
                .filter_by(run_id=self.run_id)\
                .order_by(TextNode.page, TextNode.x_indent)\
                .all()

            stack = []
            for node in nodes:
                while stack and stack[-1][0] >= node.x_indent:
                    stack.pop()
                node.parent_uid = stack[-1][1] if stack else None
                stack.append((node.x_indent, node.uid))

            session.commit()

if __name__ == "__main__":
    from src.database.db import init_db
    init_db()  # Ensures tables exist
    scraper = PDFScraper('data/chapter4.pdf')
    run_id = scraper.scrape()
    print(f"\nScraping finished! Run ID: {run_id}")
    print("   → Check logs above for details")
    print("   → View excerpts: open storage/pdfs/{run_id}/")
    print("   → Query DB: sqlite3 project.db \"SELECT COUNT(*) FROM text_nodes;\"")