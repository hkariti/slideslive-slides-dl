import argparse
import re
import os
import requests
import pandas as pd
import xml.etree.ElementTree as et
import json
import time


def parse_xml(xml_file, df_cols):
    """Parse the input XML file and store the result in a pandas
    DataFrame with the given columns.

    Features will be parsed from the text content
    of each sub-element.

    based on:
    https://medium.com/@robertopreste/from-xml-to-pandas-dataframes-9292980b1c1c
    """
    xtree = et.parse(xml_file)
    xroot = xtree.getroot()
    rows = []

    for node in xroot:
        res = []
        for el in df_cols[0:]:
            if node is not None and node.find(el) is not None:
                res.append(node.find(el).text)
            else:
                res.append(None)
        rows.append({df_cols[i]: res[i]
                     for i, _ in enumerate(df_cols)})

    out_df = pd.DataFrame(rows, columns=df_cols)

    return out_df

def parse_json(json_file):
    parsed = json.load(json_file)
    slides = dict(slides=[])

    for slide in parsed['slides']:
        slides['slides'].append(dict(slideName=slide['image']['name'], slideExt=slide['image']['extname'], time=slide['time']))

    return slides

def get_video_id(video_url):
    ids = re.findall('https://slideslive\\.(com|de)/([0-9]*)/([^/?]*)(.*)', video_url)
    if len(ids) < 1:
        print('Error: {0} is not a correct url.'.format(video_url))
        exit()
    return ids[0][1], ids[0][2]


def download_save_file(url, save_path, headers, wait_time=0.2):
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    with open(save_path, 'wb') as f:
        f.write(r.content)
    time.sleep(wait_time)

def download_slides_info(base_json_url, base_xml_url, video_id, video_name, headers, wait_time):
    folder_name = '{0}-{1}'.format(video_id, video_name)
    if not os.path.exists(folder_name):
        os.mkdir(folder_name)
    if os.path.isfile(folder_name):
        print('Error: {0} is a file, can\'t create a folder with that name'.format(folder_name))
        exit()

    try:
        info_json = download_slides_info_json(base_json_url, video_id, folder_name, headers, wait_time)
        return parse_json(info_json)
    except:
        print("Failed downloading JSON, falling back to XML")
        xml = download_slides_info_xml(base_xml_url, video_id, folder_name, headers, wait_time)
        df = parse_xml(xml, ['orderId', 'timeSec', 'time', 'slideName'])
        return df

def download_slides_info_xml(base_xml_url, video_id, download_folder, headers, wait_time):
    file_path = '{0}/{1}.xml'.format(download_folder, video_id)
    if not os.path.exists(file_path):
        xml_url = '{0}/{1}/{1}.xml'.format(base_xml_url, video_id)
        print('downloading {}'.format(file_path))
        download_save_file(xml_url, file_path, headers, wait_time)

    return open(file_path, 'r')

def download_slides_info_json(base_url, video_id, download_folder, headers, wait_time):
    file_path = '{0}/slides.json'.format(download_folder, video_id)
    if not os.path.exists(file_path):
        url = '{0}/{1}/v6/slides.json'.format(base_url, video_id)
        print('downloading {}'.format(file_path))
        download_save_file(url, file_path, headers, wait_time)

    return open(file_path, 'r')

def download_slides_json(video_id, video_name, slide_info, size, base_url, headers, wait_time):
    base_img_url = base_url + '/{video_id}/slides/{slide_name}?h={size}&f=webp&s=lambda&accelerate_s3=1'

    folder_name = '{0}-{1}'.format(video_id, video_name)
    for slide in slide_info['slides']:
        slide_name = slide['slideName'] + slide['slideExt']
        img_url = base_img_url.format(video_id=video_id, slide_name=slide_name, size=size)
        file_path = '{0}/{1}-{2}-{3}{4}'.format(folder_name, slide['slideName'], size, slide['time'], slide['slideExt'])
        print('downloading {}'.format(file_path))
        try:
            download_save_file(img_url, file_path, headers, wait_time)
        except Exception as e:
            print("Skipping due to error: {}".format(e))

def download_slides_xml(video_id, video_name, slide_info, size, base_url, headers, wait_time):
    base_img_url = base_url + '/{0}/slides/{2}/{1}.jpg'

    folder_name = '{0}-{1}'.format(video_id, video_name)
    for index, row in slide_info.iterrows():
        img_url = base_img_url.format(video_id, row['slideName'], size)
        file_path = '{0}/{3}-{1}-{2}.jpg'.format(folder_name, row['slideName'], size, row['time'])
        print('downloading {}'.format(file_path))
        try:
            download_save_file(img_url, file_path, headers, wait_time)
        except Exception as e:
            print("Skipping due to error: {}".format(e))


def create_ffmpeg_concat_file(video_id, video_name, slides_iterator, size):
    folder_name = '{0}-{1}'.format(video_id, video_name)
    ffmpeg_file_path = '{0}/ffmpeg_concat.txt'.format(folder_name)
    if os.path.exists(ffmpeg_file_path):
        return
    with open(ffmpeg_file_path, 'a') as f:
        last_time = 0
        last_file_path = ''
        for index, row in slides_iterator:
            # if not first, write duration
            time_sec = row.get('timeSec', row['time']/1000)
            duration = int(time_sec - last_time)
            if index != 0:
                f.write('duration {0}\n'.format(duration))
            file_path = '{3}-{1}-{2}.jpg'.format(folder_name, row['slideName'], size, row['time'])
            f.write("file '{0}'\n".format(file_path))
            last_time = int(time_sec)
            last_file_path = file_path
        # add some time for the last slide, we have no information how long it should be shown.
        f.write('duration 30\n')
        # Due to a quirk, the last image has to be specified twice - the 2nd time without any duration directive
        # see: https://trac.ffmpeg.org/wiki/Slideshow
        # still not bug free
        f.write("file '{0}'\n".format(last_file_path))


if __name__ == __main__:
    parser = argparse.ArgumentParser()
    parser.add_argument('url')
    parser.add_argument('--size', default='big', help='medium, big or height in pixels (new videos only)')
    parser.add_argument('--useragent', default='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/76.0.3809.100 Chrome/76.0.3809.100 Safari/537.36')
    parser.add_argument('--baseolddataurl', default='https://d2ygwrecguqg66.cloudfront.net/data/presentations', help="Base URL for old XML-based slides")
    parser.add_argument('--basedataurl', default='https://d1qcbvwoy8vxsg.cloudfront.net/')
    parser.add_argument('--basemetadataurl', default='https://slides.slideslive.com')
    parser.add_argument('--waittime', default='0.2', type=float, help='seconds to wait after each download')
    args = parser.parse_args()

    args.basedataurl = args.basedataurl.rstrip('/')
    args.basemetadataurl = args.basemetadataurl.rstrip('/')
    headers = {'User-Agent': args.useragent}

    video_id, video_name = get_video_id(args.url)
    slides_info = download_slides_info(args.basemetadataurl, args.basemetadataurl, video_id, video_name, headers, args.waittime)
    if isinstance(slides_info, dict):
        # New-style json info
        size_conversion = dict(big=1080, medium=540)
        size = size_conversion.get(args.size, args.size)
        download_slides_json(video_id, video_name, slides_info, size, args.basedataurl, headers, args.waittime)
        create_ffmpeg_concat_file(video_id, video_name, enumerate(slides_info['slides']), args.size)
    else:
        print("Failed to download using JSON metadata, falling back to XML")
        download_slides_xml(video_id, video_name, slides_info, args.size, args.baseolddataurl, headers, args.waittime)
        create_ffmpeg_concat_file(video_id, video_name, slides_info.iterrows(), args.size)
