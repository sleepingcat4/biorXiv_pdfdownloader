import requests
import pyarrow.parquet as pq
import time
import os

def download_biorXiv():
    doi_input = input("Enter a DOI (e.g., 10.1101/001891) or a Parquet filename: ")
    
    if isinstance(doi_input, str):
        if doi_input.endswith('.parquet') and os.path.isfile(doi_input):
            table = pq.read_table(doi_input)
            if 'doi' not in table.column_names:
                raise ValueError("The Parquet file does not have a 'doi' column.")
            dois = table.column('doi').to_pylist()
        else:
            dois = [doi_input]
    else:
        raise ValueError("Input must be either a DOI string or a valid Parquet file with a 'doi' column.")

    log_file = "failed_downloads.txt"
    pdf_folder = "biorXiv_pdf"
    
    if not os.path.exists(pdf_folder):
        os.makedirs(pdf_folder)

    for i, doi in enumerate(dois):
        success = False
        attempt = 0
        while not success and attempt < 2:
            try:
                pdf_url = f"https://www.biorxiv.org/content/{doi}.full.pdf"
                response = requests.get(pdf_url)

                if response.status_code == 200:
                    # Replace slashes in DOI with underscores for the filename
                    sanitized_doi = doi.replace('/', '_')
                    pdf_name = os.path.join(pdf_folder, f"{sanitized_doi}.pdf")
                    with open(pdf_name, 'wb') as f:
                        f.write(response.content)
                    print(f"Download index [{i}] and saved [{pdf_name}]")
                    success = True
                else:
                    raise Exception(f"Failed to download DOI: {doi}, Status Code: {response.status_code}")
                
                time.sleep(2)
                
            except Exception as e:
                attempt += 1
                if attempt == 1:
                    print(f"Retrying DOI: {doi} in 10 seconds due to error: {str(e)}")
                    time.sleep(10)
                else:
                    print(f"Failed to download after retrying. Logging DOI: {doi}")
                    with open(log_file, 'a') as log:
                        log.write(f"Index [{i}], DOI [{doi}] - Failed\n")
                    break

download_biorXiv()
