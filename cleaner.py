import os
import csv

folder_path = os.getcwd() + "/runs"
altitude = ''

for file_name in os.listdir(folder_path):
    if file_name.endswith('.csv'):
        print(f"Processing file: {file_name}")
        with open(os.path.join(folder_path, file_name[:-4] + "cleaned.csv"), 'w', newline='') as file_cleaned:
            with open(os.path.join(folder_path, file_name), 'r') as file:
                
                reader = csv.reader(file)
                writer = csv.writer(file_cleaned)
                
                # Read and write the first row (header) without modification
                header = next(reader)
                writer.writerow(header)
                
                # Initialize a list to hold 10 rows at a time
                rows_buffer = []
                
                for row in reader:
                    rows_buffer.append(row)
        
                    # When 10 rows are collected, process them (add altitude to the first 9 rows)
                    if len(rows_buffer) == 10:
                        # Get the altitude (assuming it's in a specific column, e.g., index 3)
                        altitude = rows_buffer[-1][3]  # Replace index 3 with the correct column index for altitude
                        
                        # Set the altitude of the previous 9 rows to match the 10th row's altitude
                        for i in range(9):
                            rows_buffer[i][3] = altitude  # Update the altitude in the previous rows
                        # Write the 10 rows to the output file
                        writer.writerows(rows_buffer)
                        
                        # Clear the buffer for the next set of 10 rows
                        rows_buffer = []