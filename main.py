from master import Downloader
import concurrent.futures

if __name__ == "__main__":
    crmware_app = Downloader()
    crmware_app.buildDFCases()  # Construir DataFrame con información de los casos
    file_names = crmware_app.dfCases['FILE_NAME'].tolist()  # Obtener nombres de archivos
    print('file_names',file_names)
    # Descargar archivos en paralelo usando hilos
    with concurrent.futures.ThreadPoolExecutor(max_workers=crmware_app.thread_count) as executor:
        executor.map(crmware_app.download_file, file_names)