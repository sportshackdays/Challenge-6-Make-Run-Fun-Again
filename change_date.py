import os

folder_path = os.getcwd() + "/runs"
file_path = os.path.join(folder_path, 'dump_300857_2024_10_06T07_45_00_000000Z.csv')
with open(file_path, 'r') as file:
    lines = file.readlines()

# Write the the original data to a backup file
with open(file_path + ".ori", 'w') as file:
    file.writelines(lines)
    
updated_lines = [(str(int(line[:10])-452) + line[10:]) for line in lines[1:]]
updated_lines.insert(0, lines[0])

# Write the updated lines back to the same file
with open(file_path, 'w') as file:
    file.writelines(updated_lines)




# if rows_buffer[i][0][:10] == '1728202248':
#     rows_buffer[i][3] = 'bla'+str(rows_buffer[i][0][10:]) #1728201795