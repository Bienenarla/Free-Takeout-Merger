## Welcome
hi hello grüezi something something

## What is this?
This program does this...

## How to use the program
to download it:
  1. check, that you use windows11 on your pc
  2. navigate to the releases section on that pc
  3. download the executeable file. nothing else is needed
  4. double click on the file. it should work :)

## How I build the executables in all releases
Here's how I built the all-in-one executeable "FreeTakeoutMerger.exe":
  1. I pasted the source files of this repo into one folder on my windows pc
  2. I installed PyInstaller via the windows command line
  3. I navigated to the source code folder (e.g. like this: "cd C:\Users\user123\Documents\sourcecode")
  4. I executed this command: python -m PyInstaller --onefile --windowed --name "FreeTakeoutMerger" --add-binary "exiftool.exe;." --add-data "exiftool_files;exiftool_files" takeout_metadata_merger_22.py

## License

This project is licensed under the GNU AGPL v3.

This project includes ExifTool by Phil Harvey,
licensed under the Artistic License 2.0 or GPL.



