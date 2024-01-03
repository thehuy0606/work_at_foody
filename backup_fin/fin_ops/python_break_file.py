import os
import math
import pandas as pd 


file_path = os.path.abspath(__file__)
current_directory = os.path.dirname(file_path)
print("Đường dẫn hiện tại:", current_directory)

file_name = str(input("Nhập tên file excel: ")) 
print(f"Bạn chọn file : {file_name}")
max_row = int(input("Nhập số dòng nhập tối đa: "))
print(f"Bạn đã nhập {max_row} dòng cho 1 file excel")
data            = pd.read_excel(f"""{current_directory}\{file_name}.xlsx""")
total_length    = len(data) 
print(f"Tổng data {total_length} chia tối đa {max_row} dòng cho 1 file")

num_file        = math.ceil(total_length / max_row)
print(f"Tiến hành chia tách thành {num_file} file")

for f in range(0, num_file):
    if f == 0:
        row_sta = 0
    else:
        row_sta = f*max_row 
    if (f+1)*max_row - 1 > total_length:
        row_end = total_length
    else:
        row_end = (f+1)*max_row - 1 
    df = data.loc[row_sta:row_end, :]

    directory = f"{current_directory}/{file_name}_batch{f+1}.xlsx"
    writer = pd.ExcelWriter(directory, engine='xlsxwriter') # , options={'encoding': 'utf-8-sig'}
    df.to_excel(writer, index=False)
    writer.close()

    print(f"start at : {row_sta} | end at : {row_end} | path : {directory} | length : {len(df)} | total amount : {'{:,.0f}'.format(df['amount'].sum())}") 
