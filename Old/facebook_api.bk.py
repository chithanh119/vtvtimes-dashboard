#!/usr/bin/env python3
"""
Script tự động patch file facebook_api.py
Thêm method get_page_current_stats() nếu chưa có
"""

import os
import re

def check_method_exists(content):
    """Kiểm tra method đã tồn tại chưa"""
    return 'def get_page_current_stats' in content

def add_method_to_file(filename='facebook_api.py'):
    """Thêm method vào file"""
    
    if not os.path.exists(filename):
        print(f"❌ File {filename} không tồn tại!")
        return False
    
    # Đọc file
    with open(filename, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Check xem method đã có chưa
    if check_method_exists(content):
        print("✅ Method get_page_current_stats() đã tồn tại!")
        return True
    
    # Tìm vị trí để insert (sau method get_page_summary_metrics)
    pattern = r'(def get_page_summary_metrics\(self\):.*?return insights)'
    
    match = re.search(pattern, content, re.DOTALL)
    
    if not match:
        print("⚠️  Không tìm thấy method get_page_summary_metrics()")
        print("    Vui lòng copy toàn bộ file mới từ artifact")
        return False
    
    # Method mới cần thêm
    new_method = '''
    
    def get_page_current_stats(self):
        """
        Lấy thống kê hiện tại của page (không phụ thuộc insights)
        
        Returns:
            Dict chứa stats hiện tại
        """
        try:
            endpoint = self.page_id
            params = {
                'fields': 'fan_count,followers_count,name,about,category'
            }
            data = self._make_request(endpoint, params)
            return data
        except Exception as e:
            print(f"Error getting page stats: {e}")
            return {}'''
    
    # Insert method mới
    insert_pos = match.end()
    new_content = content[:insert_pos] + new_method + content[insert_pos:]
    
    # Backup file cũ
    backup_file = filename + '.backup'
    with open(backup_file, 'w', encoding='utf-8') as f:
        f.write(content)
    print(f"✅ Đã backup file cũ: {backup_file}")
    
    # Ghi file mới
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(new_content)
    
    print(f"✅ Đã thêm method get_page_current_stats() vào {filename}")
    return True

if __name__ == "__main__":
    print("=" * 60)
    print("FACEBOOK API PATCH SCRIPT")
    print("=" * 60)
    print()
    
    if add_method_to_file():
        print()
        print("=" * 60)
        print("✅ PATCH COMPLETED SUCCESSFULLY")
        print("=" * 60)
        print()
        print("Next step:")
        print("  python save_facebook_data.py")
    else:
        print()
        print("=" * 60)
        print("❌ PATCH FAILED")
        print("=" * 60)
        print()
        print("Khuyến nghị:")
        print("  Copy toàn bộ file facebook_api.py mới từ artifact")