from PIL import Image
import os

# Folder where the PNG files are stored
folder = "animation"

# Get list of all PNG files in the folder, sorted by their filenames
png_files = sorted([f for f in os.listdir(folder) if f.endswith('.png')])

png_files = png_files[30:]

# Load all images
images = [Image.open(os.path.join(folder, file)) for file in png_files]

# Save as an animated GIF
output_path = "output_animation.gif"
images[0].save(output_path, save_all=True, append_images=images[1:], duration=200, loop=0)

print(f"GIF saved as {output_path}")
