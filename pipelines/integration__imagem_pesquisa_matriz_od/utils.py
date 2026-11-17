# -*- coding: utf-8 -*-

from PIL import ImageFont


def load_font(name: str, size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    """Carrega uma fonte TrueType; se não encontrar, usa a padrão do Pillow."""
    try:
        return ImageFont.truetype(name, size)
    except OSError:
        print(f"Aviso: fonte '{name}' não encontrada; usando a fonte padrão.")
        # o parâmetro `size` só existe no Pillow >= 10.1
        return ImageFont.load_default(size=size)
