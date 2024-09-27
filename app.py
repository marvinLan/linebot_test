import os
import boto3
import pandas as pd
from geopy.distance import geodesic
from flask import Flask, request, jsonify
from linebot import LineBotApi, WebhookHandler
from linebot.models import MessageEvent, ImageMessage, TextSendMessage
from PIL import Image
from datetime import datetime
import piexif
from aws_secretsmanager_caching import SecretCache, SecretCacheConfig
import json

app = Flask(__name__)

# 從 AWS Secrets Manager 取得憑證
def get_aws_credentials():
    session = boto3.session.Session()
    client = session.client('secretsmanager')
    secret_name = "YOUR_SECRET_NAME"

    secret_value = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(secret_value['SecretString'])
    return secret['AWS_ACCESS_KEY'], secret['AWS_SECRET_KEY'], secret['AWS_REGION']

aws_access_key, aws_secret_key, aws_region = get_aws_credentials()

# 初始化 AWS 客戶端
rekognition_client = boto3.client('rekognition', region_name=aws_region)
dynamodb = boto3.resource('dynamodb', region_name=aws_region)
table = dynamodb.Table('YOUR_DYNAMODB_TABLE')

# LineBot 設置 (這裡的密鑰也可使用 Secrets Manager 儲存)
line_bot_api = LineBotApi('YOUR_CHANNEL_ACCESS_TOKEN')
handler = WebhookHandler('YOUR_CHANNEL_SECRET')

# 1. 從圖片提取座標
def get_image_geolocation(image_path):
    image = Image.open(image_path)
    exif_data = piexif.load(image.info['exif'])
    gps_info = exif_data['GPS']
    lat = gps_info[2]
    lng = gps_info[4]
    # 轉換成十進位格式
    lat_dec = lat[0] + lat[1]/60 + lat[2]/3600
    lng_dec = lng[0] + lng[1]/60 + lng[2]/3600
    return lat_dec, lng_dec

# 2. 使用座標查找最近的公路段與里程
def find_closest_road_marker(lat, lng, csv_file_path):
    df = pd.read_csv(csv_file_path)
    input_coords = (lat, lng)
    df['距離'] = df.apply(lambda row: geodesic(input_coords, (row['Y座標'], row['X座標'])).meters, axis=1)
    closest_row = df.loc[df['距離'].idxmin()]
    closest_road = closest_row['公路編號']
    closest_mileage = closest_row['牌面內容']
    return closest_road, closest_mileage

# 3. 使用 AWS Rekognition 辨識災害類型
def detect_disaster_type(image_bytes):
    response = rekognition_client.detect_labels(Image={'Bytes': image_bytes}, MaxLabels=10)
    labels = [label['Name'] for label in response['Labels']]
    disaster_type = None
    if 'Landslide' in labels:
        disaster_type = '道路坍方'
    elif 'Rockfall' in labels:
        disaster_type = '落石'
    elif 'Mudslide' in labels:
        disaster_type = '土石流'
    
    people_detected = 'Person' in labels
    vehicles_detected = 'Vehicle' in labels
    return disaster_type, people_detected, vehicles_detected

# 4. 處理 LINE 圖片訊息事件
@handler.add(MessageEvent, message=ImageMessage)
def handle_image_message(event):
    message_content = line_bot_api.get_message_content(event.message.id)
    image_path = 'temp_image.jpg'
    
    with open(image_path, 'wb') as fd:
        for chunk in message_content.iter_content():
            fd.write(chunk)
    
    # 從圖片提取座標
    lat, lng = get_image_geolocation(image_path)

    # 查找最近的公路與里程
    closest_road, closest_mileage = find_closest_road_marker(lat, lng, '/mnt/data/(11309)14省道公路路線里程牌(指45)KMZ.csv')

    # 辨識災害類型與現場是否有人車
    with open(image_path, 'rb') as image_file:
        image_bytes = image_file.read()
        disaster_type, people_detected, vehicles_detected = detect_disaster_type(image_bytes)

    # 生成報告
    photo_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    report_id = f"P{closest_road}{photo_time[:4]}0001"  # 根據公路編號與年份生成流水號
    upload_time = datetime.now().isoformat()

    # 確認訊息
    description = f"災害類型: {disaster_type}, 坐標: 經度 {lng}, 緯度 {lat}, 公路段: {closest_road} {closest_mileage}, " \
                  f"照片時間: {photo_time}, 是否有人車: {'有人' if people_detected else '無人'}, {'有車' if vehicles_detected else '無車'}"
    
    # 存入 DynamoDB
    table.put_item(
        Item={
            'report_id': report_id,
            'disaster_type': disaster_type,
            'location': f"{closest_road} {closest_mileage}",
            'coordinates': {'lat': str(lat), 'lng': str(lng)},
            'timestamp': upload_time,
            'created_at': photo_time,
            'reporter_id': event.source.user_id,
            'description': description
        }
    )
    
    # 回傳確認訊息給使用者
    line_bot_api.reply_message(
        event.reply_token,
        TextSendMessage(text=description)
    )

if __name__ == "__main__":
    app.run()

