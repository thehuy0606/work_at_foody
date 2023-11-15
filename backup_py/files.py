import os

def delete_file(link_file):
    os.remove(link_file)

def export_csv(data, filename, linkdir):
    # data.to_csv(f"{linkdir}/{filename}.csv", index=False)
    directory = f"D:/powerbi/{linkdir}/{filename}.csv"
    print(directory)
    data.to_csv(directory, index=False, encoding='utf-16', sep='\t')

def export_excel(data, filename, linkdir):
    # data.to_excel(f"{linkdir}/{filename}.xlsx", index=False, engine='openpyxl')
    directory = f"D:/powerbi/{linkdir}/{filename}.xlsx"
    print(directory)
    writer = pd.ExcelWriter(directory, engine='xlsxwriter', options={'encoding': 'utf-8-sig'})
    data.to_excel(writer, index=False)
    writer.close()
    
    