import glob
import pandas as pd



#fungsi untuk membaca dataset berbagai format untuk identifikasi nama dataset menjadi nama tabel serta kolom menjadi nama kolom tabel

class ReadDataset:
    def __init__(self, file_path):
        self.file_path = file_path
        self.file_list = glob.glob(self.file_path + '/*')
        


    def hitung_dataset(self):
        # Mendapatkan daftar file di folder

        # Menghitung jumlah file
        self.jumlah_file = len(self.file_list)
        return self.jumlah_file

    def read_dataset(self):
        # Mendapatkan daftar file di folder

        # Membaca setiap file lalu menghasilkan nama tabel dan nama kolom secara looping
        for file in self.file_list:
            if file.endswith('.csv'):
                self.nama_tabel = file.split('/')[-1].split('.')[0]  
                df = pd.read_csv(file)
                self.list_kolom = df.columns.tolist()  
                
            elif file.endswith('.xlsx'):
                nama_tabel = file.split('/')[-1].split('.')[0]  
                df = pd.read_excel(file)
                self.list_kolom = df.columns.tolist()  
            elif file.endswith('.json'):
                nama_tabel = file.split('/')[-1].split('.')[0]  
                df = pd.read_json(file)
                self.list_kolom = df.columns.tolist()  
            else:
                break

        