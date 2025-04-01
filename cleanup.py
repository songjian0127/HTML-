import os

def filter_rows(input_file, output_file):
    """
    Reads an input file with rows formatted as:
      <folder name>/<file name>,<label of class_6>,<label of class_20>,<label of class_82>
    and writes a new file with rows for which the file exists on disk.
    """
    with open(input_file, 'r') as fin, open(output_file, 'w') as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            # Each row is expected to be comma separated; first field is the file path.
            parts = line.split(',')
            file_path = parts[0].strip()
            # Check if the file exists on disk
            if os.path.exists(file_path):
                fout.write(line + "\n")
            else:
                print(f"File not found, skipping row: {file_path}")

if __name__ == '__main__':
    # Define input and output file names. Original files are kept intact.
    input_test = "yoga_test.txt"
    input_train = "yoga_train.txt"
    
    output_test = "filtered_yoga_test.txt"
    output_train = "filtered_yoga_train.txt"
    
    print("Processing test file...")
    filter_rows(input_test, output_test)
    
    print("Processing train file...")
    filter_rows(input_train, output_train)
    
    print("Filtering complete. New files created:")
    print(f" - {output_test}")
    print(f" - {output_train}")
