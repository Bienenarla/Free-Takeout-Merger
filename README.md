## Welcome
Hi, I made this program(disclaimer: code was mostly written by AI) because I couldnt find any automatic metadata mergers that were free, easy and up-to-date when i got my messy files from google takeout, so I just made a program that does that.

## What is this?
This program merges photos and videos and more from Google Fotos, if you have the separated Google Takeout files, together again with their metadata files.  
Thats it, but it does a pretty decent job, it has a Graphic User Interface(=GUI), is easy to operate, works in 2026 and is completely free.


## How to use the program
to download it:
  1. check, that you use windows11 on your pc
  2. navigate to the releases section on that pc
  3. download the executeable file. nothing else is needed
  4. double click on the file. it should work :)

to use it:
  1. download it
  2. choose your settings in the GUI and then press "start"
  3. note: you can also switch to german language

## How I build the executables in the newer releases
Here's how I built the all-in-one executeable "FreeTakeoutMerger.exe":
  1. I pasted the source files of this repo into one folder on my windows pc
  2. I installed PyInstaller via the windows command line
  3. I navigated to the source code folder (e.g. like this: "cd C:\Users\user123\Documents\sourcecode")
  4. I executed this command: python -m PyInstaller --onefile --windowed --name "FreeTakeoutMerger" --add-binary "exiftool.exe;exiftool" --add-data "exiftool_files;exiftool/exiftool_files" takeout_metadata_merger.py

## License

This project is licensed under the GNU AGPL v3.

This project includes ExifTool by Phil Harvey,
licensed under the Artistic License 2.0 or GPL.



