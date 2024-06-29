import os

def compare_files(file1_path, file2_path):
    with open(file1_path, 'r') as file1, open(file2_path, 'r') as file2:
        file1_lines = file1.readlines()
        file2_lines = file2.readlines()

        # Compare line by line
        differences = []
        max_lines = max(len(file1_lines), len(file2_lines))

        for i in range(max_lines):
            line1 = file1_lines[i].rstrip('\n') if i < len(file1_lines) else ''
            line2 = file2_lines[i].rstrip('\n') if i < len(file2_lines) else ''

            if line1 != line2:
                differences.append((i + 1, line1, line2))

        return differences

def compare_directories(dir1, dir2):
    # Get list of files from both directories
    files1 = os.listdir(dir1)
    files2 = os.listdir(dir2)
    
    # Filter files based on naming convention 'databasename-tablename.sql'
    files1_filtered = [f for f in files1 if f.endswith('.sql')]
    files2_filtered = [f for f in files2 if f.endswith('.sql')]
    
    # Iterate through filtered files and compare
    for filename in set(files1_filtered) & set(files2_filtered):
        file1_path = os.path.join(dir1, filename)
        file2_path = os.path.join(dir2, filename)
        
        # Compare files line by line
        differences = compare_files(file1_path, file2_path)
        if differences:
            print(f"Files {filename} are not identical:")
            for diff in differences:
                line_num, line1, line2 = diff
                print(f"Line {line_num}:\n- {file1_path}: {line1}\n- {file2_path}: {line2}")
            raise ValueError(f"Files {filename} are not identical.")
        else:
            print(f"Files {filename} are identical.")
    
    print("All files are compared.")

# Example usage:
if __name__ == "__main__":
    directory1 = "D:\\DB Dump\\Two"
    directory2 = "D:\\DB Dump\\Try"
    
    compare_directories(directory1, directory2)
