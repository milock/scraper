import xml.etree.ElementTree as ET
import requests
import pandas as pd
import os

# Function to extract sitemaps from a sitemap index XML
def extract_sitemaps_from_index(content):
    try:
        tree = ET.ElementTree(ET.fromstring(content))
        root = tree.getroot()
        namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        sitemaps = []
        for sitemap in root.findall('ns:sitemap', namespace):
            loc = sitemap.find('ns:loc', namespace).text
            sitemaps.append(loc)
        return sitemaps
    except Exception as e:
        print(f"Failed to parse sitemap index: {e}")
        return []

# Function to extract URLs from a sitemap XML
def extract_urls_from_sitemap(content):
    try:
        tree = ET.ElementTree(ET.fromstring(content))
        root = tree.getroot()
        namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        urls = []
        for url in root.findall('ns:url', namespace):
            loc = url.find('ns:loc', namespace).text
            urls.append(loc)
        return urls
    except Exception as e:
        print(f"Failed to parse sitemap: {e}")
        return []

# Load the initial XML file
sitemap_path = 'sitemap.xml'  # Ensure this matches the path in your repository
with open(sitemap_path, 'r') as file:
    initial_xml_content = file.read()

# Extract initial sitemap URLs from the provided XML file
initial_sitemaps = extract_sitemaps_from_index(initial_xml_content)
print(f"Extracted {len(initial_sitemaps)} initial sitemap URLs from the XML file.")

# List to store all URLs
all_urls = []

# List to store sitemap URLs to process
sitemaps_to_process = initial_sitemaps[:]

batch_size = 1000  # Save progress every 1000 URLs
file_count = 1     # To keep track of the number of files
max_lines_per_file = 1_000_000  # Maximum lines per CSV file
current_file_lines = 0

output_file = f'all_sitemap_urls_{file_count}.csv'

# Ensure the output file exists and is empty
if os.path.exists(output_file):
    os.remove(output_file)

try:
    # Loop through each sitemap URL and extract nested sitemaps or URLs
    while sitemaps_to_process:
        current_sitemap_url = sitemaps_to_process.pop(0)
        try:
            response = requests.get(current_sitemap_url)
            response.raise_for_status()
            if '<sitemapindex' in response.text:
                nested_sitemaps = extract_sitemaps_from_index(response.content)
                sitemaps_to_process.extend(nested_sitemaps)
                print(f"Found {len(nested_sitemaps)} nested sitemaps in {current_sitemap_url}.")
            else:
                sitemap_urls = extract_urls_from_sitemap(response.content)
                all_urls.extend(sitemap_urls)
                print(f"Found {len(sitemap_urls)} URLs in {current_sitemap_url}.")

                # Save progress periodically
                if len(all_urls) >= batch_size:
                    df = pd.DataFrame(all_urls, columns=['URL'])
                    current_file_lines += len(all_urls)
                    if os.path.exists(output_file):
                        df.to_csv(output_file, mode='a', header=False, index=False)
                    else:
                        df.to_csv(output_file, index=False)
                    all_urls = []  # Clear the list after saving

                    # Check if the current file has reached the maximum line limit
                    if current_file_lines >= max_lines_per_file:
                        file_count += 1
                        output_file = f'all_sitemap_urls_{file_count}.csv'
                        current_file_lines = 0

        except Exception as e:
            print(f"Failed to scrape {current_sitemap_url}: {e}")

    # Save any remaining URLs
    if all_urls:
        df = pd.DataFrame(all_urls, columns=['URL'])
        current_file_lines += len(all_urls)
        if os.path.exists(output_file):
            df.to_csv(output_file, mode='a', header=False, index=False)
        else:
            df.to_csv(output_file, index=False)

except KeyboardInterrupt:
    print("Script interrupted by user. Saving progress...")
    if all_urls:
        df = pd.DataFrame(all_urls, columns=['URL'])
        current_file_lines += len(all_urls)
        if os.path.exists(output_file):
            df.to_csv(output_file, mode='a', header=False, index=False)
        else:
            df.to_csv(output_file, index=False)

print(f"All URLs have been extracted and saved to {file_count} CSV file(s).")
