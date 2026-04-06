from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from typing import Union

import click
import pypdf

from pdf_watermark.draw import draw_watermarks_to_bytes
from pdf_watermark.options import (
    DrawingOptions,
    FilesOptions,
    GridOptions,
    InsertOptions,
)
from pdf_watermark.utils import convert_content_to_images, sort_pages


def add_watermark_to_pdf(
    input: str,
    output: str,
    drawing_options: DrawingOptions,
    specific_options: Union[GridOptions, InsertOptions],
):
    pdf_writer = pypdf.PdfWriter()
    pdf_to_transform = pypdf.PdfReader(input)

    page_sizes = []
    for page in pdf_to_transform.pages:
        page_sizes.append((page.mediabox.width, page.mediabox.height))

    order = []

    for watermark_width, watermark_height in set(page_sizes):
        watermark_pdf_bytes = draw_watermarks_to_bytes(
            watermark_width,
            watermark_height,
            drawing_options,
            specific_options,
        )

        watermark_pdf = pypdf.PdfReader(BytesIO(watermark_pdf_bytes))

        for index, (page, (page_width, page_height)) in enumerate(
            zip(pdf_to_transform.pages, page_sizes)
        ):
            if page_width == watermark_width and page_height == watermark_height:
                page.merge_page(watermark_pdf.pages[0])
                pdf_writer.add_page(page)
                order.append(index)

    pdf_writer = sort_pages(pdf_writer, order)

    with open(output, "wb") as f:
        pdf_writer.write(f)

    if drawing_options.unselectable and not drawing_options.save_as_image:
        convert_content_to_images(output, drawing_options.dpi)

    if drawing_options.save_as_image:
        convert_content_to_images(output, drawing_options.dpi)


def add_watermark_from_options(
    files_options: FilesOptions,
    drawing_options: DrawingOptions,
    specific_options: Union[GridOptions, InsertOptions],
):
    def process_file(input_file, output_file):
        if files_options.verbose or files_options.dry_run:
            if input_file == output_file:
                click.echo(f"modifying: {output_file}")
            else:
                click.echo(f"creating: {output_file}")
        if not files_options.dry_run:
            add_watermark_to_pdf(
                input_file, output_file, drawing_options, specific_options
            )

    if files_options.workers > 1:
        with ThreadPoolExecutor(max_workers=files_options.workers) as executor:
            futures = [
                executor.submit(process_file, inp, out) for inp, out in files_options
            ]
            for future in futures:
                future.result()
    else:
        for input_file, output_file in files_options:
            process_file(input_file, output_file)
