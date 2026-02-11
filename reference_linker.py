import re
import logging
from src.database.db import get_session
from src.database.models import TextNode, Run

import os
import json
import fitz
from collections import defaultdict
from src.database.models import TextNode, Run

STORAGE_ROOT = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'storage')

logger = logging.getLogger(__name__)

class ReferenceLinker:
    def __init__(self, run_id: int):
        self.run_id = run_id
        # UPDATED REGEX:
        # 1. (\d+(?:\.\d+)+(?:\([a-zA-Z0-9]+\))*) -> Matches "4.4.3" optionally followed by "(5)" or "(5)(a)"
        # 2. |\([a-zA-Z0-9]+\) -> OR matches standalone "(a)"
        self.ref_pattern = r'(\d+(?:\.\d+)+(?:\([a-zA-Z0-9]+\))*|\([a-zA-Z0-9]+\))'

    def run(self):
        logger.info(f"Starting reference linking for Run ID: {self.run_id}")
        
        with get_session() as session:
            nodes = session.query(TextNode).filter_by(run_id=self.run_id).all()
            
            # Map normalized codes (e.g., '4_4_3_5') to UIDs
            rule_map = {n.rule_code: n.uid for n in nodes if n.rule_code}
            
            links_created = 0

            for node in nodes:
                matches = re.findall(self.ref_pattern, node.text)
                unique_matches = set(matches)
                
                for match in unique_matches:
                    # Normalize: "4.4.3(5)" -> "4_4_3_5"
                    clean_match = match.replace('.', '_').replace('(', '_').replace(')', '').strip('_')

                    if clean_match in rule_map and rule_map[clean_match] != node.uid:
                        target_uid = rule_map[clean_match]
                        target_node = session.query(TextNode).get(target_uid)
                        
                        if target_node not in node.outgoing_references:
                            node.outgoing_references.append(target_node)
                            links_created += 1
            
            session.commit()
            logger.info(f"Linking complete. Created {links_created} cross-references.")
            return links_created


class HierarchyProcessor:
    def __init__(self, run_id: int):
        self.run_id = run_id
        self.top_level_indent = 90.0
        self.tolerance = 1.0

    def run(self):
        with get_session() as session:
            # 1. Fetch all nodes for this run
            nodes = session.query(TextNode).filter_by(run_id=self.run_id).order_by(TextNode.node_index).all()
            
            # 2. Assign top_level_uid based on indentation
            current_top_level_uid = None
            for node in nodes:
                is_at_header_margin = abs(node.x_indent - self.top_level_indent) <= self.tolerance
                
                # Update the header if it's at the margin AND NOT a Note
                if is_at_header_margin and node.type != 'NOTE':
                    current_top_level_uid = node.uid
                
                node.top_level_uid = current_top_level_uid
            
            session.commit()
            logger.info("Hierarchy assigned. Generating unified single-page snippets...")

            # 3. Generate the contextual snippets stitched into one page
            self._generate_cross_page_snippets(session, nodes)
            
            return len(nodes)

    def _generate_cross_page_snippets(self, session, nodes):
        run = session.query(Run).get(self.run_id)
        pdf_doc = fitz.open(run.pdf_path)
        storage_path = os.path.join(STORAGE_ROOT, 'pdfs', str(self.run_id))
        os.makedirs(storage_path, exist_ok=True)

        # Group all nodes by their top_level_uid
        hierarchy_map = defaultdict(list)
        for node in nodes:
            if node.top_level_uid:
                hierarchy_map[node.top_level_uid].append(node)

        for tl_uid, group_nodes in hierarchy_map.items():
            # Organize nodes within this hierarchy by page
            page_groups = defaultdict(list)
            for n in group_nodes:
                page_groups[n.page].append(n)
            
            sorted_pages = sorted(page_groups.keys())

            # --- Phase A: Pre-calculate total dimensions and crop rectangles ---
            crops_info = []
            total_height = 0
            max_width = 0
            
            for p_num in sorted_pages:
                nodes_on_page = page_groups[p_num]
                page_idx = p_num - 1
                src_page = pdf_doc[page_idx]
                
                bboxes = [json.loads(n.bbox_json) for n in nodes_on_page if n.bbox_json]
                if not bboxes: continue
                
                # Determine vertical bounds with a small padding
                y_min = max(0, min(b[1] for b in bboxes) - 10)
                y_max = min(src_page.rect.height, max(b[3] for b in bboxes) + 10)
                
                crop_rect = fitz.Rect(0, y_min, src_page.rect.width, y_max)
                
                crops_info.append({
                    'p_num': p_num,
                    'p_idx': page_idx,
                    'crop_rect': crop_rect,
                    'height': crop_rect.height,
                    'width': crop_rect.width
                })
                total_height += crop_rect.height
                max_width = max(max_width, crop_rect.width)

            # --- Phase B: Generate the stitched snippet for each node ---
            for target_node in group_nodes:
                new_doc = fitz.open()
                # Create exactly ONE page for the whole merged hierarchy
                snippet_page = new_doc.new_page(width=max_width, height=total_height)
                
                current_y_offset = 0
                for info in crops_info:
                    # Destination rectangle on our new merged page
                    dest_rect = fitz.Rect(0, current_y_offset, info['width'], current_y_offset + info['height'])
                    
                    # Copy the clipped fragment to the specific vertical offset
                    snippet_page.show_pdf_page(dest_rect, pdf_doc, info['p_idx'], clip=info['crop_rect'])

                    # Highlight ONLY the specific target_node
                    if info['p_num'] == target_node.page:
                        target_bbox = json.loads(target_node.bbox_json)
                        
                        # Calculate vertical position relative to the crop and the current offset
                        rel_y_top = target_bbox[1] - info['crop_rect'].y0
                        rel_y_bot = target_bbox[3] - info['crop_rect'].y0
                        
                        highlight = fitz.Rect(
                            target_bbox[0], 
                            current_y_offset + rel_y_top, 
                            target_bbox[2], 
                            current_y_offset + rel_y_bot
                        )
                        snippet_page.draw_rect(highlight, color=None, fill=(1, 1, 0), fill_opacity=0.3)
                    
                    current_y_offset += info['height']

                # Create a snake-case clean code (e.g., 4.1.2 -> 4_1_2)
                clean_code = str(target_node.rule_code) if target_node.rule_code else "no_code"
                output_filename = f"node_{target_node.node_index}_{clean_code}.pdf"
                
                # Save final single-page snippet
                new_doc.save(os.path.join(storage_path, output_filename))
                new_doc.close()

        pdf_doc.close()