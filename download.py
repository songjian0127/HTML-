import os
import requests
import concurrent.futures

def download_single_image(file_spec, url):
    """
    Downloads a single image given its file specification and URL.
    If the download fails with the given URL, it attempts to switch the protocol
    (http <-> https) and try again.
    The image is saved to <folder>/<filename>, creating the folder if needed.
    """
    folder, filename = os.path.split(file_spec)
    if folder and not os.path.exists(folder):
        os.makedirs(folder, exist_ok=True)
    
    def try_download(download_url):
        response = requests.get(download_url, stream=True)
        response.raise_for_status()  # Raises HTTPError for bad responses
        file_output_path = os.path.join(folder, filename)
        with open(file_output_path, 'wb') as out_file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    out_file.write(chunk)
    
    try:
        try_download(url)
        return url  # Return the URL used successfully
    except Exception as e:
        # Attempt alternate protocol if possible
        alt_url = None
        if url.startswith("http://"):
            alt_url = "https://" + url[len("http://"):]
        elif url.startswith("https://"):
            alt_url = "http://" + url[len("https://"):]
        
        if alt_url:
            print(f"Original URL failed: {url} with error: {e}. Trying alternate URL: {alt_url}")
            try:
                try_download(alt_url)
                return alt_url
            except Exception as e2:
                raise Exception(f"Both original ({url}) and alternate ({alt_url}) URLs failed. "
                                f"Original error: {e}, Alternate error: {e2}")
        else:
            raise Exception("URL does not start with http:// or https://")

def download_images_from_file(file_path, failed_downloads):
    """
    Reads a text file with lines formatted as:
      <folder name>/<file name><tab><URL>
    For each line, it downloads the image using a ThreadPoolExecutor to enforce a 30-second timeout.
    If a download fails (including timeouts or protocol issues), the failure is recorded.
    """
    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            # Split on a tab to allow file names with spaces.
            try:
                file_spec, url = line.split('\t', 1)
            except ValueError:
                error_msg = f"Skipping invalid line in {file_path}: {line}"
                print(error_msg)
                failed_downloads.append({'file': None, 'url': None, 'error': error_msg})
                continue
            
            # Use a ThreadPoolExecutor to enforce the 30-second timeout.
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(download_single_image, file_spec, url)
                try:
                    result_url = future.result(timeout=30)
                    folder, filename = os.path.split(file_spec)
                    print(f"Downloaded '{filename}' to '{folder}' using URL: {result_url}")
                except concurrent.futures.TimeoutError:
                    error_msg = f"Timeout for {url} after 30 seconds"
                    print(error_msg)
                    failed_downloads.append({'file': file_spec, 'url': url, 'error': error_msg})
                except Exception as e:
                    error_msg = f"Failed to download {url}. Error: {e}"
                    print(error_msg)
                    failed_downloads.append({'file': file_spec, 'url': url, 'error': str(e)})

def download_all_txt_files(directory):
    """
    Iterates over all .txt files in the specified directory and processes them.
    Returns a list of downloads that failed.
    """
    failed_downloads = []
    for file in os.listdir(directory):
        if file.endswith('.txt'):
            file_path = os.path.join(directory, file)
            print(f"Processing {file_path}...")
            download_images_from_file(file_path, failed_downloads)
    return failed_downloads

if __name__ == '__main__':
    # Change this to the directory where your txt files are located.
    txt_files_directory = 'yoga_dataset_links'
    failures = download_all_txt_files(txt_files_directory)
    
    # Report the failures
    if failures:
        print("\nDownload failures:")
        for failure in failures:
            file_info = failure['file'] if failure['file'] else "Unknown file specification"
            print(f"- File: {file_info}, URL: {failure['url']}, Error: {failure['error']}")
        print(f"\nTotal failures: {len(failures)}")
    else:
        print("\nAll downloads completed successfully.")
