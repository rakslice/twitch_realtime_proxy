#!/usr/bin/env python2.7
import argparse
import socket
import time
import BaseHTTPServer
import urllib


""" The dumbest possible proxy for a single Twitch real time stream """


def contents(url):
    handle = urllib.urlopen(url)
    try:
        return handle.read()
    finally:
        handle.close()


PREFETCH_PREFIX = "#EXT-X-TWITCH-PREFETCH:"


def get_prefetch_urls(data):
    """
    Extract prefetch URLs from m3u8 contents provided
    """
    out = []
    for line in data.split("\n"):
        if line.startswith(PREFETCH_PREFIX):
            url = line[len(PREFETCH_PREFIX):]
            out.append(url)
    return out


def contents_stream(url, piece_size):
    """
    Download the given url and yield it in chunks of the given size
    """
    handle = urllib.urlopen(url)
    try:
        while True:
            data = handle.read(piece_size)
            if data == "":
                break
            yield data
    finally:
        handle.close()


def short_url(url):
    """
    Truncate url for display
    """
    return url[:72] + "[...]"


class MyHandler(BaseHTTPServer.BaseHTTPRequestHandler):

    def __init__(self, *args, **kwargs):
        self.stream_client_connected = False
        BaseHTTPServer.BaseHTTPRequestHandler.__init__(self, *args, **kwargs)

    # noinspection PyPep8Naming
    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()

    # noinspection PyPep8Naming
    def do_GET(self):
        if self.stream_client_connected:
            self.send_response(503)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write("Only one stream connection at a time is allowed")
            return

        playlist_url = self.path[1:]
        print playlist_url

        data = contents(playlist_url)
        print "Playlist:"
        print data
        last_playlist_time = time.time()

        prefetch_urls = get_prefetch_urls(data)

        last_chunk_loaded = None

        assert prefetch_urls != []

        self.stream_client_connected = True

        """Respond to a GET request."""
        self.send_response(200)
        self.send_header("Content-type", "video/MP2T")
        self.end_headers()

        cycles_with_no_new_pieces = 0

        block_size = 4096

        while True:
            print "processing playlist fetched at %s" % last_playlist_time
            next_playlist_time = last_playlist_time + 1.9
            try:
                next_chunk_pos = prefetch_urls.index(last_chunk_loaded) + 1
            except ValueError:
                next_chunk_pos = 0

            connection_closed = False

            new_prefetch_urls = prefetch_urls[next_chunk_pos:]

            if len(new_prefetch_urls) == 0:
                cycles_with_no_new_pieces += 1
            else:
                cycles_with_no_new_pieces = 0

            if cycles_with_no_new_pieces > 3:
                print "Stream appears to be over"
                break

            for i, url in enumerate(new_prefetch_urls):
                cur_chunk_size = 0
                print "serving entry %d %s" % (next_chunk_pos + i, short_url(url))
                for piece in contents_stream(url, block_size):
                    try:
                        self.wfile.write(piece)
                        cur_chunk_size += len(piece)
                    except socket.error, e:
                        if e.errno == 10053:
                            # client closed the connection normally
                            connection_closed = True
                            break
                        raise
                last_chunk_loaded = url

                # Adjust to a larger block size for higher bitrate streams
                # so this isn't too inefficient
                byterate = float(cur_chunk_size) / 2.0
                block_time = float(block_size) / byterate
                print "byterate %0.2f, calculated block time %0.3f" % (byterate, block_time)
                if block_time < 0.0625:
                    while block_time < 0.0625:
                        block_size *= 2
                        block_time = float(block_size) / byterate
                    print "increased block size to %s" % block_size

            if connection_closed:
                break

            if time.time() < next_playlist_time:
                print "waiting for next playlist time"
                while time.time() < next_playlist_time:
                    time.sleep(0.125)

            print "loading next playlist"
            data = contents(playlist_url)
            last_playlist_time = time.time()
            prefetch_urls = get_prefetch_urls(data)

        print "Processing stream client connection complete"
        self.stream_client_connected = False


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--listen-address", "-l",
                        default="127.0.0.1",
                        help="IP address to listen on")
    parser.add_argument("--port", "-p",
                        default=12380,
                        type=int,
                        help="port to listen on")

    return parser.parse_args()


def main():
    options = parse_args()
    listen_address = options.listen_address
    listen_port = options.port

    server_class = BaseHTTPServer.HTTPServer
    httpd = server_class((listen_address, listen_port), MyHandler)
    print time.asctime(), "Server Starts - %s:%s" % (listen_address, listen_port)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()
    print time.asctime(), "Server Stops - %s:%s" % (listen_address, listen_port)


if __name__ == '__main__':
    main()
