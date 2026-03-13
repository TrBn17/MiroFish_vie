"""Dich vu xu ly van ban."""

from typing import List, Optional
from ..utils.file_parser import FileParser, split_text_into_chunks


class TextProcessor:
    """Bo xu ly van ban."""
    
    @staticmethod
    def extract_from_files(file_paths: List[str]) -> str:
        """Trich xuat van ban tu nhieu tep."""
        return FileParser.extract_from_multiple(file_paths)
    
    @staticmethod
    def split_text(
        text: str,
        chunk_size: int = 500,
        overlap: int = 50
    ) -> List[str]:
        """
        Chia nho van ban.

        Args:
            text: Van ban goc.
            chunk_size: Kich thuoc moi doan.
            overlap: So ky tu chong lap giua hai doan.

        Returns:
            Danh sach cac doan van ban.
        """
        return split_text_into_chunks(text, chunk_size, overlap)
    
    @staticmethod
    def preprocess_text(text: str) -> str:
        """
        Tien xu ly van ban.
        - Loai bo khoang trang thua
        - Chuan hoa ky tu xuong dong

        Args:
            text: Van ban goc.

        Returns:
            Van ban sau xu ly.
        """
        import re
        
        # Chuan hoa ky tu xuong dong
        text = text.replace('\r\n', '\n').replace('\r', '\n')
        
        # Loai bo nhieu dong trong lien tiep, chi giu toi da hai dau xuong dong
        text = re.sub(r'\n{3,}', '\n\n', text)
        
        # Loai bo khoang trang o dau va cuoi moi dong
        lines = [line.strip() for line in text.split('\n')]
        text = '\n'.join(lines)
        
        return text.strip()
    
    @staticmethod
    def get_text_stats(text: str) -> dict:
        """Lay thong tin thong ke cua van ban."""
        return {
            "total_chars": len(text),
            "total_lines": text.count('\n') + 1,
            "total_words": len(text.split()),
        }

