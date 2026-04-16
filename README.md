**WELCOME and blah blah**
*I give no guarantee that this program works as intended on your computer.

**What is this?**
This program will do this...

Here's how I built the all-in-one executeable "FreeTakeoutMerger.exe": 
  1. I pasted the source files of this repo into one folder on my windows pc
  2. I installed PyInstaller via the windows command line
  3. I navigated to the source code folder (e.g. like this: "cd C:\Users\user123\Documents\sourcecode_folder")
  4. I executed this command: python -m PyInstaller --onefile --windowed --name "FreeTakeoutMerger" --add-binary "exiftool.exe;." --add-data "exiftool_files;exiftool_files" takeout_metadata_merger_22.py

*Legal Notices (again):*
  *This project includes ExifTool by Phil Harvey*
  *https://exiftool.org/*

  *Licensed under the Artistic License 2.0*



