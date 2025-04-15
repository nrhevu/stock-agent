import json
import re

def clean_text(text):
    # Loại bỏ các ký tự thừa như \r, \n và khoảng trắng thừa
    if text is None:
        return text
    text = re.sub(r'\s+', ' ', text.strip())
    return text

def extract_date(text):
    # Chỉ giữ lại ngày tháng năm trong publish_date
    if text is None:
        return text
    date_match = re.search(r'(\d{2}/\d{2}/\d{4})', text)
    return date_match.group(1) if date_match else text

def clean_json_data(json_data):
    cleaned_data = []
    for item in json_data:
        content = clean_text(item.get("content", ""))
        if "nvidia" not in content and "Nvidia" not in content:
            continue
        cleaned_item = {
            "title": clean_text(item.get("title", "")),
            "publish_date": extract_date(item.get("publish_date", "")),
            "content": clean_text(item.get("content", ""))
        }
        cleaned_data.append(cleaned_item)
    return cleaned_data

if __name__ == "__main__":
    # Đọc file JSON
    with open("nvidia.json", "r", encoding="utf-8") as f:
        data = json.load(f)
    print(data)
    # Làm sạch dữ liệu
    cleaned_data = clean_json_data(data)

    # Ghi dữ liệu đã làm sạch ra file mới
    with open("cleaned_data.json", "w", encoding="utf-8") as f:
        json.dump(cleaned_data, f, ensure_ascii=False, indent=2)

    print("Dữ liệu đã được làm sạch và lưu vào 'cleaned_data.json'")