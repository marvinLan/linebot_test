import os
import boto3
import json
from flask import Flask, request, abort
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, ImageMessage, TextSendMessage
from aws_secretsmanager_caching import SecretCache, SecretCacheConfig
from PIL import Image
from PIL.ExifTags import TAGS

# 初始化 Flask 應用
app = Flask(__name__)

# 設定 AWS Secrets Manager 的客戶端
secrets_client = boto3.client('secretsmanager', region_name='YOUR_AWS_REGION')
cache_config = SecretCacheConfig()
cache = SecretCache(config=cache_config, client=secrets_client)

# 抓取金鑰資料（從 AWS Secrets Manager 中讀取）
secret_name = 'YOUR_SECRET_NAME'
secret = json.loads(cache.get_secret_string(secret_name))

# 從密鑰中讀取 LINE 和 AWS 金鑰
line_bot_api = LineBotApi(secret['line_channel_access_token'])
handler = WebhookHandler(secret['line_channel_secret'])
aws_access_key_id = secret['aws_access_key_id']
aws_secret_access_key = secret['aws_secret_access_key']
s3_bucket_name = secret['s3_bucket_name']
dynamodb_table_name = secret['dynamodb_table_name']

# 初始化 Rekognition 和 DynamoDB 客戶端
rekognition_client = boto3.client(
    'rekognition',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name='YOUR_AWS_REGION'
)
dynamodb_client = boto3.resource(
    'dynamodb',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name='YOUR_AWS_REGION'
)
s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name='YOUR_AWS_REGION'
)
table = dynamodb_client.Table(dynamodb_table_name)

# LINE Webhook Callback
@app.route("/callback", methods=['POST'])
def callback():
    signature = request.headers['X-Line-Signature']
    body = request.get_data(as_text=True)
    app.logger.info(f"Request body: {body}")

    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)

    return 'OK'

# 接收來自 LINE 的圖片訊息
@handler.add(MessageEvent, message=ImageMessage)
def handle_image_message(event):
    message_content = line_bot_api.get_message_content(event.message.id)
    image_bytes = message_content.content
    
    # 將圖片存入 S3
    s3_key = f"disaster_photos/{event.message.id}.jpg"
    s3_client.put_object(Bucket=s3_bucket_name, Key=s3_key, Body=image_bytes)
    
    # 用 AWS Rekognition 辨識圖片
    response = rekognition_client.detect_labels(
        Image={'Bytes': image_bytes},
        MaxLabels=10,
        MinConfidence=75
    )
    
    disaster_type = classify_disaster(response['Labels'])
    
    # 抓取照片的時間與座標（用 EXIF 提取）
    image = Image.open(image_bytes)
    photo_time, lat, lng = extract_exif_data(image)
    
    # 確認並回傳資料給用戶
    report_message = generate_report_message(
        disaster_type=disaster_type,
        lat=lat, 
        lng=lng, 
        photo_time=photo_time
    )
    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=report_message))

    # 確認後將資料寫入 DynamoDB
    report_id = generate_report_id()
    upload_time = "2024-09-24T17:05:00Z"  # 這裡可以用 time 模組來自動生成

    table.put_item(
        Item={
            'report_id': report_id,
            'disaster_type': disaster_type,
            'coordinates': {'lat': str(lat), 'lng': str(lng)},
            'timestamp': upload_time,
            'created_at': photo_time,
            'reporter_id': event.source.user_id,
            's3_photo_key': s3_key  # 儲存 S3 的 Key 以便後續查詢
        }
    )

# 災害類型的分類邏輯
def classify_disaster(labels):
    disaster_types = ["Rockfall", "Road Collapse", "Landslide"]
    for label in labels:
        if label['Name'] in disaster_types:
            return label['Name']
    return "Unknown"

# 生成回報訊息
def generate_report_message(disaster_type, lat, lng, photo_time):
    return f"災害類型：{disaster_type}\n座標及定位：經度: {lng}, 緯度: {lat}\n照片時間：{photo_time}"

# 生成報告編號
def generate_report_id():
    return f"R{str(32).zfill(4)}"  # 假設 32 是報告的序號

# 提取 EXIF 數據
def extract_exif_data(image):
    exif_data = image._getexif()
    photo_time = None
    lat = None
    lng = None
    if exif_data:
        for tag, value in exif_data.items():
            tag_name = TAGS.get(tag, tag)
            if tag_name == 'DateTimeOriginal':
                photo_time = value
            if tag_name == 'GPSInfo':
                gps_info = value
                lat = convert_gps(gps_info[2])  # 轉換 GPS 座標
                lng = convert_gps(gps_info[4])
    return photo_time, lat, lng

# 將 GPS 座標轉換為十進位制
def convert_gps(gps_data):
    degrees = gps_data[0][0] / gps_data[0][1]
    minutes = gps_data[1][0] / gps_data[1][1]
    seconds = gps_data[2][0] / gps_data[2][1]
    return degrees + (minutes / 60.0) + (seconds / 3600.0)

if __name__ == "__main__":
    app.run()
