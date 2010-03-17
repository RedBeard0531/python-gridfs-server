#!/usr/bin/env python2
from bottle import *
import pymongo
import mimetypes
from itertools import imap
from operator import itemgetter

DB_NAME = 'test'
COLLECTION_PREFIX = 'fs'
CONNECTION = pymongo.Connection() # add host and port if needed
SERVER = AutoServer # see bottle.py for possible servers
DEBUG = False # don't print exceptions in http output

db = CONNECTION[DB_NAME] 
fs = db[COLLECTION_PREFIX]

@get('/:filename#.*#')
def serve_file(filename):
# Most of this is based on bottle.py's static_file function but modified for GridFS
    try: 
        file = fs.files.find({'filename': filename}).sort('uploadDate', -1).limit(1)[0]
    except IndexError:
        return HTTPError(404, "Not found:" + filename)

    headers = {}

    if 'contentType' in file:
        headers['Content-Type'] = file['contentType']
    else:
        headers['Content-Type'] = mimetypes.guess_type(filename)[0]

    headers['Content-Length'] = file['length']
    headers['Last-Modified'] = file['uploadDate'].strftime("%a, %d %b %Y %H:%M:%S GMT")
    headers['ETag'] = str(file['md5'])

    #TODO: something nice with etag
       #return HTTPResponse("Not modified", status=304, header=header)

    if request.method == 'HEAD':
        return HTTPResponse('', header=headers)
    else:
        chunks = fs.chunks.find({'files_id': file['_id']}).sort('n')
        chunks = imap(itemgetter('data'), chunks)
        chunks = imap(str, chunks)
        return HTTPResponse(chunks, header=headers)

if __name__ == '__main__':
    debug(DEBUG)
    run(server=SERVER)
