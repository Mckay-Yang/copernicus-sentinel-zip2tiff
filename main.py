import os
import threading
import zipfile
from datetime import datetime
from bs4 import BeautifulSoup
from osgeo import gdal, osr

BANDS_SAVE_RESOLUTION = {
    # 'B01': '60m',
    'B02': '10m',
    'B03': '10m',
    'B04': '10m',
    # 'B05': '20m',
    # 'B06': '20m',
    # 'B07': '20m',
    'B08': '10m',
    # 'B8A': '20m',
    # 'B09': '60m',
    'B11': '20m',
    'B12': '20m',
    # 'AOT': '10m',
    # 'WVP': '10m',
    # 'SCL': '20m',
    # 'TCI': '10m',
}

def _get_bands_image_path(extract_dir: str) -> dict:
    bands_image_path = {}
    for root, dirs, files in os.walk(extract_dir):
        for file in files:
            if file.endswith('.jp2'):
                band = file.split('_')[-2]
                resolution = file.split('_')[-1].replace('.jp2', '')
                if band in BANDS_SAVE_RESOLUTION and BANDS_SAVE_RESOLUTION[band] == resolution:
                    bands_image_path[band] = os.path.join(root, file)
    bands_image_path = dict(sorted(bands_image_path.items()))
    return bands_image_path

def _get_meta(template_band_path: str) -> dict:
    dataset = gdal.Open(template_band_path)
    meta = {
        'driver': 'GTiff',
        'width': dataset.RasterXSize,
        'height': dataset.RasterYSize,
        'count': dataset.RasterCount,
        'dtype': gdal.GetDataTypeName(dataset.GetRasterBand(1).DataType),
        'crs': dataset.GetProjection(),
        'transform': dataset.GetGeoTransform(),
    }
    return meta

def _resample_band(band_path: str, reference_band_path: str) -> str:
    resampled_band_path = band_path.replace('.jp2', '_resampled.tif')
    src_ds = gdal.Open(band_path)
    ref_ds = gdal.Open(reference_band_path)
    ref_transform = ref_ds.GetGeoTransform()
    ref_projection = ref_ds.GetProjection()
    ref_width = ref_ds.RasterXSize
    ref_height = ref_ds.RasterYSize

    dst_ds = gdal.GetDriverByName('GTiff').Create(
        resampled_band_path, ref_width, ref_height, src_ds.RasterCount, src_ds.GetRasterBand(1).DataType
    )
    dst_ds.SetGeoTransform(ref_transform)
    dst_ds.SetProjection(ref_projection)

    gdal.ReprojectImage(src_ds, dst_ds, src_ds.GetProjection(), ref_projection, gdal.GRA_Bilinear)
    dst_ds.FlushCache()
    return resampled_band_path

def _get_image_time(xml_path: str) -> tuple:
    tree = BeautifulSoup(open(xml_path), 'xml')
    system_time_start = tree.find('PRODUCT_START_TIME').text
    system_time_end = tree.find('PRODUCT_STOP_TIME').text
    start_time_ms = int(datetime.strptime(system_time_start, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp() * 1000)
    end_time_ms = int(datetime.strptime(system_time_end, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp() * 1000)
    print(start_time_ms, end_time_ms)
    return start_time_ms, end_time_ms

def _produce_tif(extract_dir: str):
    bands_image_path = _get_bands_image_path(extract_dir)
    if not bands_image_path:
        print(f'Error: No bands image found in {extract_dir}')
        return

    xml_path = os.path.join(extract_dir, 'MTD_MSIL2A.xml')

    meta = _get_meta(bands_image_path['B02'])
    if not meta:
        print(f'Error: No meta found in {extract_dir}')
        return

    tiff_name = extract_dir.split('/')[-1]
    out_tiff_path = extract_dir.replace(extract_dir.split('/')[-1], '') + tiff_name + '.tif'
    meta.update({
        'driver': 'GTiff',
        'count': len(bands_image_path),
        'bigtiff': 'YES'
    })

    driver = gdal.GetDriverByName('GTiff')
    dst_ds = driver.Create(out_tiff_path, meta['width'], meta['height'], meta['count'], gdal.GDT_UInt16, ['BIGTIFF=YES'])
    dst_ds.SetGeoTransform(meta['transform'])
    dst_ds.SetProjection(meta['crs'])

    for i, band in enumerate(bands_image_path.keys(), 1):
        print(f'Processing {band}...')
        band_path = bands_image_path[band]
        if BANDS_SAVE_RESOLUTION[band] != '10m':
            band_path = _resample_band(band_path, bands_image_path['B02'])
        src_ds = gdal.Open(band_path)
        dst_ds.GetRasterBand(i).WriteArray(src_ds.GetRasterBand(1).ReadAsArray())
        dst_ds.GetRasterBand(i).SetDescription(band)

    system_time_end, system_time_start = _get_image_time(xml_path)
    dst_ds.SetMetadata({
        'system-time_start': f'{system_time_start}',
        'system-time_end': f'{system_time_end}',
    })

    dst_ds.FlushCache()
    print(f'Successfully created {out_tiff_path}')

def _process_zip_to_tif_thread(zip_file: str, zip_dir: str, output_dir: str, sem: threading.Semaphore):
    try:
        print(f'Processing {zip_file}...')
        zip_path = os.path.join(zip_dir, zip_file)
        extract_dir = os.path.join(output_dir, zip_file.replace('.zip', ''))
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(output_dir)
            _produce_tif(extract_dir)
    finally:
        sem.release()

def process_zip_to_tif(zip_dir: str, output_dir: str, max_threads: int):
    if not os.path.exists(zip_dir):
        print(f'Error: {zip_dir} does not exist')
        return

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    zip_files_list = [f for f in os.listdir(zip_dir) if f.endswith('.zip')]

    sem = threading.Semaphore(max_threads)

    for zip_file in zip_files_list:
        sem.acquire()
        thread = threading.Thread(target=_process_zip_to_tif_thread, args=(zip_file, zip_dir, output_dir, sem))
        thread.start()

if __name__ == '__main__':
    zip_dir = '../../Datasets/sentinel2_l2a_yarlunzangbo_downstream/'
    output_dir = './output'
    max_threads = 10
    process_zip_to_tif(zip_dir, output_dir, max_threads)