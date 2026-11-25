# -*- coding: utf-8 -*-

import re
from pathlib import Path

from google.api_core.exceptions import NotFound
from google.cloud import storage
from PIL import Image, ImageDraw, ImageFont

from pipelines.integration__imagem_pesquisa_matriz_od import constants


def load_font(name: str, size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    """
    Carrega uma fonte TrueType; se não encontrar, usa a padrão do Pillow.

    Args:
        name (str): Nome da fonte.
        size (int): Tamanho da fonte.

    Returns:
        (ImageFont.FreeTypeFont | ImageFont.ImageFont): Fonte carregada
    """
    try:
        return ImageFont.truetype(name, size)
    except OSError:
        print(f"Aviso: fonte '{name}' não encontrada; usando a fonte padrão.")
        return ImageFont.load_default(size=size)


def annotate_image(  # noqa: PLR0913
    image: Image.Image,
    letter: str,
    font: ImageFont.FreeTypeFont | ImageFont.ImageFont,
    caption_font: ImageFont.FreeTypeFont | ImageFont.ImageFont,
    caption: str | None = None,
    padding: int = 15,
) -> Image.Image:
    """
    Desenha a letra (canto superior esquerdo) e a legenda (canto inferior esquerdo) na imagem
    do stop

    Args:
        image (Image.Image): Imagem do stop.
        letter (str): Letra a ser desenhada.
        font (ImageFont.FreeTypeFont | ImageFont.ImageFont): Fonte da letra.
        caption_font (ImageFont.FreeTypeFont | ImageFont.ImageFont): Fonte da legenda.
        caption (str | None): Texto da legenda.
        padding (int): Tamanho do do espaçamento entre os textos e a borda do retângulo
    """
    draw = ImageDraw.Draw(image)

    left, top, right, bottom = draw.textbbox((0, 0), letter, font=font)
    box_w, box_h = (right - left) + 30, (bottom - top) + 30
    draw.rounded_rectangle(
        (padding, padding, padding + box_w, padding + box_h), radius=8, fill="black"
    )
    draw.text((padding + 15 - left, padding + 15 - top), letter, font=font, fill="white")

    if caption:
        cl, ct, cr, cb = draw.textbbox((0, 0), caption, font=caption_font)
        cw, ch = cr - cl, cb - ct
        px, py = 12, 8
        x0 = padding
        y0 = image.height - ch - 2 * py - padding
        draw.rounded_rectangle((x0, y0, x0 + cw + 2 * px, y0 + ch + 2 * py), radius=6, fill="black")
        draw.text((x0 + px - cl, y0 + py - ct), caption, font=caption_font, fill="white")

    return image


def build_choice_grid(
    panels_spec: list[tuple[str, Path, str | None]],
    out_path: Path,
    font: ImageFont.FreeTypeFont | ImageFont.ImageFont,
    caption_font: ImageFont.FreeTypeFont | ImageFont.ImageFont,
    gap: int = 10,
) -> Path:
    """
    Monta a grade 2x2 de imagens.

    Args:
        panels_spec (list[tuple[str, Path, str | None]]): lista de tuplas com os itens:
            - letra;
            - caminho_da_imagem;
            - legenda.
            na ordem A, B, C, D.
        out_path (Path): Caminho para salvar a imagem final.
        font (ImageFont.FreeTypeFont | ImageFont.ImageFont): Fonte da letra.
        caption_font (ImageFont.FreeTypeFont | ImageFont.ImageFont): Fonte da legenda.
        gap (int): Tamanho do espaçamento entre as imagens.

    Returns:
        Path: O caminho em que a imagem foi salva.
    """

    images = []
    for letter, path, caption in panels_spec:
        with Image.open(path) as img:
            images.append((letter, img.convert("RGB"), caption))

    w = min(img.width for _, img, _ in images)
    h = min(img.height for _, img, _ in images)

    canvas = Image.new("RGB", (2 * w + gap, 2 * h + gap), color="white")

    for i, (letter, img, caption) in enumerate(images):
        resized_img = img.resize((w, h), Image.Resampling.LANCZOS)
        resized_img = annotate_image(
            resized_img,
            letter,
            font,
            caption_font,
            caption,
        )

        col, row = i % 2, i // 2
        canvas.paste(resized_img, (col * (w + gap), row * (h + gap)))

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    canvas.save(out_path, format="PNG")
    return out_path


def download_image_from_gcs(
    bucket_obj: storage.Bucket,
    stop_id: str,
    local_folder: str,
    downloaded_images: list,
) -> Path:
    """
    Faz o download de uma imagem de stop no GCS

    Args:
        bucket_obj (storage.Bucket): Objeto de Bucket GCS.
        stop_id (str): Identificador do stop no GTFS.
        local_folder (str): Pasta para salvar as imagens.
        downloaded_images (list): Lista contendo os stop_ids que já foram baixados.

    Returns:
        Path: Caminho da imagem salva
    """
    blob_name = f"{constants.GCS_IMAGE_SOURCE_DIR}/stop_id_{stop_id}.png"
    blob = bucket_obj.blob(blob_name)
    local_folder = Path(local_folder)
    local_path = local_folder / Path(blob_name).name

    if stop_id in downloaded_images:
        return local_path

    local_folder.mkdir(parents=True, exist_ok=True)

    blob.download_to_filename(str(local_path))

    downloaded_images.append(stop_id)

    return local_path


def build_journey_image(  # noqa: PLR0913
    bucket_obj: storage.Bucket,
    journey_key: str,
    stop_info: dict[str, str],
    font: ImageFont.FreeTypeFont | ImageFont.ImageFont,
    caption_font: ImageFont.FreeTypeFont | ImageFont.ImageFont,
    downloaded_images: list,
) -> tuple[Path | None, list[str]]:
    """
    Gera a imagem final com as 4 opções de desembarque.

    Args:
        bucket_obj (storage.Bucket): Objeto de Bucket GCS.
        journey_key (str): Identificador da viagem.
        stop_info (dict[str, str]): Dicionário no formato {<stop_id>: <stop_name>}.
        font (ImageFont.FreeTypeFont | ImageFont.ImageFont): Fonte da letra.
        caption_font (ImageFont.FreeTypeFont | ImageFont.ImageFont): Fonte da legenda.
        downloaded_images (list): Lista contendo os stop_ids que já foram baixados.

    Returns:
        Path: Caminho em que a imagem foi salva.
    """

    stop_ids = stop_info.keys()

    if not re.fullmatch(r"[A-Za-z0-9_-]+", journey_key):
        raise ValueError("journey_key deve conter apenas letras, dígitos, '_' ou '-'.")

    if len(stop_ids) != len(constants.IMAGE_LETTERS):
        raise ValueError("stop_ids deve conter exatamente quatro IDs.")

    paths = []
    no_image_stop_ids = []

    for sid in stop_ids:
        try:
            path = download_image_from_gcs(
                bucket_obj=bucket_obj,
                stop_id=sid,
                local_folder=constants.IMAGE_DIR,
                downloaded_images=downloaded_images,
            )
            paths.append(path)
        except NotFound:
            no_image_stop_ids.append(sid)

    if len(no_image_stop_ids) > 0:
        return None, no_image_stop_ids

    spec = [
        (letter, path, stop_info.get(sid))
        for letter, path, sid in zip(constants.IMAGE_LETTERS, paths, stop_ids, strict=False)
    ]

    out_path = build_choice_grid(
        panels_spec=spec,
        out_path=constants.OUT_DIR / f"{journey_key}.png",
        font=font,
        caption_font=caption_font,
    )

    return out_path, no_image_stop_ids


def upload_image_to_gcs(bucket_obj: storage.Bucket, image_local_path: Path):
    """
    Faz upload das imagens finais para o GCS.

    Args:
        bucket_obj (storage.Bucket): Objeto de Bucket GCS.
        image_local_path (Path): Pasta local com a imagem.
    """
    blob_name = f"{constants.GCS_OUT_DIR}/{image_local_path.name}"
    blob = bucket_obj.blob(blob_name)
    blob.upload_from_filename(str(image_local_path))
