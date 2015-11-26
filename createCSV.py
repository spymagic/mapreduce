import os
import csv
import json

def get_filepaths(directory):
	file_paths = []

	for root,dirs, files in os.walk(directory):
		for filename in files:
			filepath=os.path.join(root,filename)
			file_paths.append(filepath)

	return file_paths

full_file_paths = get_filepaths("C:\Users\Pengyi\Downloads\lastfm_train")

outfile = csv.writer(open('C:\Users\Pengyi\Downloads\output.csv', 'w'))
#with open('/home/spymagic/final/output.csv', 'w') as outfile:
for fname in full_file_paths:
		with open(fname) as infile:
			data = json.load(infile)

			if len(data["tags"])!=0:
				#parse the first tag
				tag = data["tags"][0][0]
				#parse the track_id
				track_id=data["track_id"]
				#parse the title
				title=data["title"]

				outfile.writerow([track_id, title.encode('utf-8'), tag.encode('utf-8')])

