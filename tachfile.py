import pandas as pd
import os
from typing import Optional, List

def filter_data_by_device(
    input_file: str,
    output_dir: str = "device_data",
    device_column: str = "device",
    file_format: str = "csv",
    specific_devices: Optional[List[str]] = None
) -> dict:
    """
    Lọc dữ liệu từ file chính và tách thành các file riêng biệt theo device.
    
    Args:
        input_file (str): Đường dẫn đến file dữ liệu gốc
        output_dir (str): Thư mục để lưu các file đầu ra (mặc định: "device_data")
        device_column (str): Tên cột chứa device ID (mặc định: "device")
        file_format (str): Định dạng file đầu ra ("csv", "json", "excel")
        specific_devices (List[str]): Danh sách device cụ thể cần lọc (None = tất cả)
    
    Returns:
        dict: Thông tin thống kê về quá trình lọc
    """
    
    try:
        # Đọc dữ liệu từ file
        print(f"Đang đọc dữ liệu từ {input_file}...")
        
        if input_file.endswith('.csv'):
            df = pd.read_csv(input_file)
        elif input_file.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(input_file)
        elif input_file.endswith('.json'):
            df = pd.read_json(input_file)
        else:
            raise ValueError("Định dạng file không được hỗ trợ. Chỉ hỗ trợ CSV, Excel, JSON")
        
        # Kiểm tra cột device có tồn tại không
        if device_column not in df.columns:
            raise ValueError(f"Không tìm thấy cột '{device_column}' trong dữ liệu")
        
        # Tạo thư mục đầu ra nếu chưa tồn tại
        os.makedirs(output_dir, exist_ok=True)
        
        # Lấy danh sách các device duy nhất
        unique_devices = df[device_column].unique()
        
        # Lọc theo device cụ thể nếu được chỉ định
        if specific_devices:
            unique_devices = [d for d in unique_devices if d in specific_devices]
            if not unique_devices:
                print("Không tìm thấy device nào trong danh sách chỉ định!")
                return {}
        
        print(f"Tìm thấy {len(unique_devices)} device(s): {list(unique_devices)}")
        
        # Thống kê
        stats = {
            'total_devices': len(unique_devices),
            'total_records': len(df),
            'devices_processed': {},
            'output_files': []
        }
        
        # Lọc và lưu dữ liệu cho từng device
        for device_id in unique_devices:
            # Lọc dữ liệu theo device
            device_data = df[df[device_column] == device_id].copy()
            
            # Tạo tên file
            safe_device_name = str(device_id).replace('/', '_').replace('\\', '_')
            
            if file_format.lower() == 'csv':
                output_file = os.path.join(output_dir, f"device_{safe_device_name}.csv")
                device_data.to_csv(output_file, index=False, encoding='utf-8')
            
            elif file_format.lower() == 'json':
                output_file = os.path.join(output_dir, f"device_{safe_device_name}.json")
                device_data.to_json(output_file, orient='records', indent=2, force_ascii=False)
            
            elif file_format.lower() == 'excel':
                output_file = os.path.join(output_dir, f"device_{safe_device_name}.xlsx")
                device_data.to_excel(output_file, index=False)
            
            else:
                raise ValueError("Định dạng file không hỗ trợ. Chỉ hỗ trợ: csv, json, excel")
            
            # Cập nhật thống kê
            stats['devices_processed'][device_id] = {
                'records_count': len(device_data),
                'output_file': output_file
            }
            stats['output_files'].append(output_file)
            
            print(f"Đã tạo file cho device {device_id}: {len(device_data)} records -> {output_file}")
        
        print(f"\nHoàn thành! Đã tạo {len(stats['output_files'])} file(s) trong thư mục '{output_dir}'")
        return stats
        
    except Exception as e:
        print(f"Lỗi: {str(e)}")
        return {}


def get_device_summary(input_file: str, device_column: str = "device") -> pd.DataFrame:
    """
    Tạo bảng tóm tắt thông tin về các device trong dữ liệu.
    
    Args:
        input_file (str): Đường dẫn đến file dữ liệu gốc
        device_column (str): Tên cột chứa device ID
    
    Returns:
        pd.DataFrame: Bảng tóm tắt thông tin device
    """
    
    try:
        # Đọc dữ liệu
        if input_file.endswith('.csv'):
            df = pd.read_csv(input_file)
        elif input_file.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(input_file)
        elif input_file.endswith('.json'):
            df = pd.read_json(input_file)
        else:
            raise ValueError("Định dạng file không được hỗ trợ")
        
        # Tạo bảng tóm tắt
        summary = df.groupby(device_column).agg({
            'id': 'count',  # Số lượng records
            'created_at': ['min', 'max'],  # Thời gian đầu và cuối
            'online': lambda x: x.sum() if 'online' in df.columns else 'N/A',  # Số lần online
            'active_time': 'sum' if 'active_time' in df.columns else 'mean'  # Tổng thời gian hoạt động
        }).round(2)
        
        # Đặt lại tên cột
        summary.columns = ['Total_Records', 'First_Record', 'Last_Record', 'Online_Count', 'Total_Active_Time']
        summary = summary.reset_index()
        
        return summary
        
    except Exception as e:
        print(f"Lỗi khi tạo bảng tóm tắt: {str(e)}")
        return pd.DataFrame()


# Ví dụ sử dụng
if __name__ == "__main__":
    # Sử dụng cơ bản
    stats = filter_data_by_device(
        input_file="device_log.csv",  # File dữ liệu gốc
        output_dir="filtered_devices",  # Thư mục đầu ra
        file_format="csv"  # Định dạng file đầu ra
    )
    
    # In thống kê
    if stats:
        print(f"\n=== THỐNG KÊ ===")
        print(f"Tổng số device: {stats['total_devices']}")
        print(f"Tổng số records: {stats['total_records']}")
        print(f"Số file đã tạo: {len(stats['output_files'])}")
        
        print(f"\nChi tiết theo device:")
        for device_id, info in stats['devices_processed'].items():
            print(f"  {device_id}: {info['records_count']} records")
    
    # Tạo bảng tóm tắt
    print(f"\n=== BẢNG TÓM TẮT DEVICE ===")
    summary = get_device_summary("data.csv")
    if not summary.empty:
        print(summary.to_string(index=False))