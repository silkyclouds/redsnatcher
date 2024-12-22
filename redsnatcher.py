import json
import time
import logging
import os
import sqlite3
import requests
from unidecode import unidecode
import colorlog
from datetime import datetime
import re
import argparse
import sys
import subprocess

from flask import Flask, request, jsonify

# -----------------------------
# Release types mapping
# -----------------------------
RELEASE_TYPE_MAP = {
    1: "Album",
    3: "Soundtrack",
    5: "EP",
    6: "Anthology",
    7: "Compilation",
    9: "Single",
    11: "Live album",
    13: "Remix",
    14: "Bootleg",
    15: "Interview",
    16: "Mixtape",
    17: "Demo",
    18: "Concert Recording",
    19: "DJ Mix",
    21: "Unknown",
    1021: "Produced By",
    1022: "Composition",
    1023: "Remixed By",
    1024: "Guest Appearance"
}

# -----------------------------
# Supported formats
# -----------------------------
SUPPORTED_FORMATS = {
    0: "MP3",
    1: "FLAC",
    2: "AAC",
    3: "AC3",
    4: "DTS"
}

# -----------------------------
# Media order
# -----------------------------
SUPPORTED_MEDIA = {
    0: "CD",
    1: "DVD",
    2: "Vinyl",
    3: "Soundboard",
    4: "SACD",
    5: "DAT",
    6: "Cassette",
    7: "WEB",
    8: "Blu-Ray"
}

def sanitize_filename(filename):
    filename = unidecode(filename)
    filename = re.sub(r'[\\/*?:"<>|]', '', filename)
    filename = re.sub(r'\s+', ' ', filename)
    return filename.strip()

class RedSnatcher:
    def __init__(self, config_path='config.json'):
        self.load_config(config_path)
        self.setup_logging()
        self.setup_database()

        self.tokens_available = self.use_freeleech_tokens_when_available
        self.already_failed_downloads = set()  # Track torrents that failed "4 times" error

        # 1) Retrieve user stats (display only).
        self.get_and_display_user_stats()

        # 2) Retrieve all previously snatched torrents from Redacted, storing in DB,
        #    now capturing groupId, name, size, artistId, etc. if present in the response.
        self.retrieve_and_insert_snatched_torrents_from_api()

        # 3) Automatically synchronize bookmarked artists from Redacted upon startup.
        self.get_bookmarked_artists()

    # -------------------------------------------------------------------------
    # Load configuration from JSON
    # -------------------------------------------------------------------------
    def load_config(self, config_path):
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"The configuration file '{config_path}' was not found.")
        with open(config_path, 'r') as f:
            config = json.load(f)

        self.redacted_api_key = config.get("redacted_api_key")
        self.redacted_api_url = config.get("redacted_api_url")
        self.user_id = config.get("user_id")
        self.log_level = config.get("log_level", "DEBUG")
        self.db_path = config.get("db_path", "redsnatcher.db")
        self.download_torrents = config.get("download_torrents", False)
        self.all_snatches_download_path = config.get("all_snatches_download_path", "./all_snatches/")
        self.artist_torrents_download_path = config.get("artist_torrents_download_path", "./artists/")
        self.selected_formats = config.get("formats", ["FLAC"])
        self.selected_release_types = config.get("download_release_types", ["Album","EP","Single","Anthology"])
        self.media_preference = config.get("media", ["WEB", "CD", "Vinyl"])
        self.re_download_snatched = config.get("re_download_snatched", False)
        self.include_similar_artists = config.get("include_similar_artists", False)
        self.use_freeleech_tokens_when_available = config.get("use_freeleech_tokens_when_available", True)
        self.snatch_freeleech_only = config.get("snatch_freeleech_only", True)
        self.privilege_releases_with_most_tracks = config.get("privilege_releases_with_most_tracks", False)
        self.max_torrent_size_mb = config.get("maximum_snatched_torrent_size", 1000)  # in MB

        # Daemon server settings
        self.daemon_host = config.get("daemon_host", "0.0.0.0")
        self.daemon_port = config.get("daemon_port", 56789)

        if not all([self.redacted_api_key, self.redacted_api_url, self.user_id]):
            raise ValueError("Fields 'redacted_api_key', 'redacted_api_url', and 'user_id' must be defined.")

    # -------------------------------------------------------------------------
    # Setup logging
    # -------------------------------------------------------------------------
    def setup_logging(self):
        formatter = colorlog.ColoredFormatter(
            "%(log_color)s%(asctime)s - %(levelname)s - %(message)s",
            datefmt='%Y-%m-%d %H:%M:%S',
            log_colors={
                'DEBUG': 'cyan',
                'INFO': 'green',
                'WARNING': 'yellow',
                'ERROR': 'red',
                'CRITICAL': 'bold_red',
            }
        )
        handler = colorlog.StreamHandler()
        handler.setFormatter(formatter)

        self.logger = logging.getLogger('RedSnatcher')
        level = logging.getLevelName(self.log_level.upper())
        if isinstance(level, int):
            self.logger.setLevel(level)
        else:
            self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(handler)
        self.logger.info("Logging configured successfully.")
        self.logger.info(f"Redacted API URL: {self.redacted_api_url}")

    # -------------------------------------------------------------------------
    # Setup database (create tables if missing)
    # -------------------------------------------------------------------------
    def setup_database(self):
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.cursor = self.conn.cursor()

        expected_schema = {
            'user_stats': {
                'timestamp': 'DATETIME PRIMARY KEY',
                'uploaded': 'INTEGER',
                'downloaded': 'INTEGER',
                'buffer': 'INTEGER',
                'ratio': 'REAL',
                'required_ratio': 'REAL',
                'user_class': 'TEXT',
                'snatched': 'INTEGER'
            },
            'bookmarked_artists': {
                'artist_id': 'INTEGER PRIMARY KEY',
                'artist_name': 'TEXT'
            },
            'snatched_torrents': {
                'torrent_id': 'INTEGER PRIMARY KEY',
                'group_id': 'INTEGER',
                'release_name': 'TEXT',
                'release_type': 'INTEGER',
                'artist_id': 'INTEGER',
                'artist_name': 'TEXT',
                'year': 'INTEGER',
                'format': 'TEXT',
                'media': 'TEXT',
                'size': 'INTEGER',
                'seeders': 'INTEGER',
                'leechers': 'INTEGER',
                'snatched': 'INTEGER',
                'time_added': 'TEXT',
                'download_url': 'TEXT'
            },
            'downloaded_torrents': {
                'torrent_id': 'INTEGER PRIMARY KEY',
                'download_path': 'TEXT',
                'downloaded_at': 'DATETIME'
            },
            'fulfilled_albums': {
                'group_id': 'INTEGER PRIMARY KEY',
                'downloaded_format': 'TEXT',
                'downloaded_media': 'TEXT',
                'fulfilled_at': 'DATETIME'
            }
        }

        for table, columns in expected_schema.items():
            self.check_and_create_table(table, columns)

        self.conn.commit()
        self.logger.info("Database initialized successfully.")

    def check_and_create_table(self, table_name, columns):
        self.cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,)
        )
        table_exists = self.cursor.fetchone()

        if not table_exists:
            columns_def = ', '.join([f"{col} {dtype}" for col, dtype in columns.items()])
            create_table_query = f"CREATE TABLE {table_name} ({columns_def})"
            self.cursor.execute(create_table_query)
            self.logger.info(f"Table '{table_name}' created successfully.")
        else:
            # Check if any columns are missing
            self.cursor.execute(f"PRAGMA table_info({table_name})")
            existing_columns = [row[1] for row in self.cursor.fetchall()]
            for col, dtype in columns.items():
                if col not in existing_columns:
                    alter_table_query = f"ALTER TABLE {table_name} ADD COLUMN {col} {dtype}"
                    self.cursor.execute(alter_table_query)
                    self.logger.info(f"Column '{col}' added to '{table_name}'.")

    # -------------------------------------------------------------------------
    # Rate limit for 2 seconds
    # -------------------------------------------------------------------------
    def rate_limit(self):
        self.logger.debug("Waiting 2 seconds to respect the rate limit.")
        time.sleep(2)

    # -------------------------------------------------------------------------
    # Step 1: Retrieve user stats
    # -------------------------------------------------------------------------
    def get_user_stats(self):
        self.logger.info("Retrieving user stats from Redacted.")
        params = {"action": "user", "id": self.user_id}
        headers = {'Authorization': self.redacted_api_key}
        self.logger.debug(f"Request: {params}")
        self.rate_limit()
        try:
            response = requests.get(self.redacted_api_url, params=params, headers=headers)
            self.logger.debug(f"Response Status: {response.status_code}")
            if response.status_code != 200:
                self.logger.error(f"Redacted API Error: {response.text}")
                return 0
            data = response.json()
            if data.get("status") != "success":
                self.logger.error(f"Redacted API Error: {data.get('error')}")
                return 0
            response_data = data.get("response", {})
            stats = response_data.get("stats", {})
            community = response_data.get("community", {})
            snatched_count = community.get("snatched", 0)
            timestamp = datetime.now()

            self.cursor.execute('''
                INSERT OR REPLACE INTO user_stats (timestamp, uploaded, downloaded, buffer, ratio, required_ratio, user_class, snatched)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                timestamp,
                stats.get("uploaded", 0),
                stats.get("downloaded", 0),
                stats.get("buffer", 0),
                stats.get("ratio", 0.0),
                stats.get("requiredRatio", 0.0),
                response_data.get("personal", {}).get("class", "Unknown"),
                snatched_count
            ))
            self.conn.commit()
            self.logger.info(
                f"User stats updated. "
                f"Ratio: {stats.get('ratio', 0.0):.2f}, "
                f"Uploaded: {stats.get('uploaded', 0)}, "
                f"Downloaded: {stats.get('downloaded', 0)}, "
                f"Buffer: {stats.get('buffer', 0)}, "
                f"Required Ratio: {stats.get('requiredRatio', 0.0):.2f}, "
                f"Snatched: {snatched_count}."
            )
            return snatched_count
        except Exception as e:
            self.logger.error(f"Error retrieving user stats: {e}")
            return 0

    def get_and_display_user_stats(self):
        snatched_count = self.get_user_stats()
        if not snatched_count:
            self.logger.info("User stats retrieval failed or returned 0 for 'snatched'.")
        else:
            self.logger.info(f"Stats retrieval completed. Total snatched reported: {snatched_count}")

    # -------------------------------------------------------------------------
    # Step 2: Retrieve the full list of snatched torrents from user_torrents
    #         Now we store groupId, name => release_name, artistId, artistName,
    #         torrentSize => size, etc.
    # -------------------------------------------------------------------------
    def retrieve_and_insert_snatched_torrents_from_api(self):
        """
        Fetch all user snatches from Redacted via user_torrents endpoint (type=snatched),
        storing them in the local DB. This version parses extra fields (groupId, name,
        artistId, artistName, torrentSize) in addition to torrentId.
        """
        self.logger.info("Fetching the full list of already snatched torrents from Redacted. Please be patient...")

        # 1) Try to get a total snatched estimate for a progress bar.
        total_snatched_estimate = 0
        try:
            params_stats = {"action": "user", "id": self.user_id}
            headers_stats = {"Authorization": self.redacted_api_key}
            self.rate_limit()
            resp = requests.get(self.redacted_api_url, params=params_stats, headers=headers_stats)
            if resp.status_code == 200:
                data_stats = resp.json()
                if data_stats.get("status") == "success":
                    total_snatched_estimate = data_stats.get("response", {}).get("community", {}).get("snatched", 0)
        except Exception as e:
            self.logger.error(f"Could not retrieve total snatched estimate: {e}")

        if total_snatched_estimate > 0:
            total_pages_estimate = (total_snatched_estimate // 500) + 1
        else:
            total_pages_estimate = 0

        limit = 500
        offset = 0
        total_fetched = 0
        current_page = 1

        while True:
            params = {
                "action": "user_torrents",
                "id": self.user_id,
                "type": "snatched",
                "limit": limit,
                "offset": offset
            }
            headers = {"Authorization": self.redacted_api_key}
            self.rate_limit()
            try:
                response = requests.get(self.redacted_api_url, params=params, headers=headers)
                if response.status_code != 200:
                    self.logger.error(f"Error retrieving user snatched torrents: {response.text}")
                    break
                data = response.json()
                if data.get("status") != "success":
                    self.logger.error(f"Error from user_torrents endpoint: {data.get('error')}")
                    break

                results = data.get("response", {}).get("snatched", [])
                if not results:
                    self.logger.info("No more snatched torrents found in this batch.")
                    break

                # Insert data
                for item in results:
                    # According to the sample JSON structure:
                    # {
                    #   "groupId": 2400012,
                    #   "name": "Rather Vexxxed II : Volume I",
                    #   "torrentId": 5254750,
                    #   "torrentSize": "315985029",
                    #   "artistName": "Mellow Thing",
                    #   "artistId": 1719563
                    # }
                    tid = int(item.get("torrentId"))
                    gid = int(item.get("groupId", 0))
                    release_name = item.get("name", "")
                    size_str = item.get("torrentSize", "0")
                    try:
                        size_val = int(size_str)
                    except:
                        size_val = 0
                    artname = item.get("artistName", "")
                    artid = int(item.get("artistId", 0))

                    # Insert or update snatched_torrents with these fields
                    self.cursor.execute('''
                        INSERT OR IGNORE INTO snatched_torrents (
                            torrent_id, group_id, release_name, artist_id, artist_name, size
                        ) VALUES (?, ?, ?, ?, ?, ?)
                    ''', (tid, gid, release_name, artid, artname, size_val))

                    total_fetched += 1

                self.conn.commit()

                # Show progress if we have an estimate
                if total_snatched_estimate > 0:
                    progress_pct = (float(current_page) / float(total_pages_estimate)) * 100.0
                    self.logger.info(f"Progress: {progress_pct:.1f}% ({total_fetched} snatches recorded)...")

                offset += limit
                current_page += 1

                # If we got fewer than 'limit', likely done.
                if len(results) < limit:
                    break

            except Exception as e:
                self.logger.error(f"Error retrieving user snatched torrents: {e}")
                break

        self.logger.info(f"Finished fetching previously snatched torrents. Total found/recorded: {total_fetched}")

    # -------------------------------------------------------------------------
    # Basic DB retrieval
    # -------------------------------------------------------------------------
    def get_user_torrents_snatched(self):
        """
        Reads from DB which torrents have already been snatched.
        """
        self.logger.info("Retrieving snatched torrents from the DB.")
        try:
            self.cursor.execute('SELECT torrent_id FROM snatched_torrents')
            rows = self.cursor.fetchall()
            snatched_torrent_ids = set(row[0] for row in rows)
            self.logger.info(f"{len(snatched_torrent_ids)} torrents already snatched in DB.")
            return snatched_torrent_ids
        except Exception as e:
            self.logger.error(f"Error retrieving snatched torrents: {e}")
            return set()

    def get_fulfilled_albums(self):
        self.logger.info("Retrieving fulfilled albums from DB.")
        try:
            self.cursor.execute('SELECT group_id FROM fulfilled_albums')
            rows = self.cursor.fetchall()
            fulfilled = set(row[0] for row in rows)
            self.logger.info(f"{len(fulfilled)} albums already fulfilled.")
            return fulfilled
        except Exception as e:
            self.logger.error(f"Error retrieving fulfilled albums: {e}")
            return set()

    def mark_album_fulfilled(self, group_id, downloaded_format, downloaded_media):
        try:
            self.cursor.execute('''
                INSERT OR REPLACE INTO fulfilled_albums (group_id, downloaded_format, downloaded_media, fulfilled_at)
                VALUES (?, ?, ?, ?)
            ''', (group_id, downloaded_format, downloaded_media, datetime.now()))
            self.conn.commit()
            self.logger.info(f"Album with GroupID {group_id} marked as fulfilled.")
        except Exception as e:
            self.logger.error(f"Error marking album {group_id} fulfilled: {e}")

    # -------------------------------------------------------------------------
    # Bookmarked Artists
    # -------------------------------------------------------------------------
    def get_bookmarked_artists(self):
        """
        Retrieve bookmarked artists from Redacted and store them in DB.
        """
        self.logger.info("Retrieving bookmarked artists from Redacted.")
        params = {"action": "bookmarks", "type": "artists"}
        headers = {'Authorization': self.redacted_api_key}
        self.logger.debug(f"Request: {params}")
        self.rate_limit()
        try:
            response = requests.get(self.redacted_api_url, params=params, headers=headers)
            self.logger.debug(f"Response Status: {response.status_code}")
            if response.status_code != 200:
                self.logger.error(f"Redacted API Error: {response.text}")
                return []
            data = response.json()
            if data.get("status") != "success":
                self.logger.error(f"Redacted API Error: {data.get('error')}")
                return []
            artists = data.get("response", {}).get("artists", [])
            artist_names = [a.get("artistName") for a in artists]
            artist_ids = [a.get("artistId") for a in artists]
            self.logger.info(f"{len(artist_names)} bookmarked artists retrieved.")
            if artist_names:
                self.logger.info(f"Artists: {', '.join(artist_names)}")
            for artist_id, artist_name in zip(artist_ids, artist_names):
                self.cursor.execute('''
                    INSERT OR IGNORE INTO bookmarked_artists (artist_id, artist_name)
                    VALUES (?, ?)
                ''', (artist_id, artist_name))
            self.conn.commit()
            return list(zip(artist_ids, artist_names))
        except Exception as e:
            self.logger.error(f"Error retrieving bookmarked artists: {e}")
            return []

    def get_all_bookmarked_artists(self):
        """
        Read from DB all bookmarked artists.
        """
        try:
            self.cursor.execute('SELECT artist_id, artist_name FROM bookmarked_artists')
            return self.cursor.fetchall()
        except Exception as e:
            self.logger.error(f"Error retrieving bookmarked artists from DB: {e}")
            return []

    # -------------------------------------------------------------------------
    # Browsing an Artist
    # -------------------------------------------------------------------------
    def browse_artist_torrents(self, artist_name):
        self.logger.info(f"Browsing torrents for artist: {artist_name}")
        results = []
        page = 1
        while True:
            params = {
                "action": "browse",
                "artistname": artist_name,
                "page": page
            }
            headers = {
                'Authorization': self.redacted_api_key
            }
            self.rate_limit()
            self.logger.debug(f"Browsing page {page} for '{artist_name}'")
            response = requests.get(self.redacted_api_url, params=params, headers=headers)
            self.logger.debug(f"Browse Response Status: {response.status_code}")
            if response.status_code != 200:
                self.logger.error(f"Redacted API Error browsing for artist '{artist_name}': {response.text}")
                break
            data = response.json()
            if data.get("status") != "success":
                self.logger.error(f"Redacted API Error browsing '{artist_name}': {data.get('error')}")
                break
            resp = data.get("response", {})
            current_page = resp.get("currentPage", 1)
            pages = resp.get("pages", 1)
            browse_results = resp.get("results", [])
            if not browse_results:
                self.logger.info(f"No torrents found for artist '{artist_name}' on page {page}.")
                break
            results.extend(browse_results)
            if current_page >= pages:
                break
            page += 1
        return results

    # -------------------------------------------------------------------------
    # Utility: Convert bytes -> MB
    # -------------------------------------------------------------------------
    def bytes_to_mb(self, size_in_bytes):
        return size_in_bytes / (1024 * 1024)

    # -------------------------------------------------------------------------
    # Selecting a preferred torrent from a list
    # -------------------------------------------------------------------------
    def select_preferred_torrent(self, torrents_list):
        # 1) Filter by user-chosen format
        candidates = []
        for fmt in self.selected_formats:
            fmts = [t for t in torrents_list if t.get("format", "") == fmt]
            if fmts:
                candidates.extend(fmts)
                break

        if not candidates:
            return None

        # 2) Filter by media in user-chosen order
        final_candidates = []
        for m in self.media_preference:
            media_matches = [t for t in candidates if t.get("media", "") == m]
            if media_matches:
                final_candidates.extend(media_matches)
                break

        if not final_candidates:
            return None

        # 3) Privilege torrent with the most fileCount
        if self.privilege_releases_with_most_tracks:
            final_candidates.sort(key=lambda x: x.get("fileCount", 0), reverse=True)

        return final_candidates[0] if final_candidates else None

    # -------------------------------------------------------------------------
    # Should we skip this torrent due to user-defined constraints?
    # -------------------------------------------------------------------------
    def should_skip_torrent(self, torrent_info):
        tid = torrent_info["torrentId"]
        size_mb = self.bytes_to_mb(torrent_info.get("size", 0))
        if size_mb > self.max_torrent_size_mb:
            self.logger.info(f"Skipping torrent ID {tid} due to size limit. Size: {size_mb:.2f}MB > {self.max_torrent_size_mb}MB")
            return True
        if self.snatch_freeleech_only:
            self.logger.debug(
                f"snatch_freeleech_only is True, checking if torrent ID {tid} is freeleech: {torrent_info.get('isFreeleech', False)}"
            )
            if not torrent_info.get("isFreeleech", False):
                self.logger.info(f"Skipping torrent ID {tid}, not freeleech and snatch_freeleech_only is enabled.")
                return True
        return False

    # -------------------------------------------------------------------------
    # Actually download a torrent
    # -------------------------------------------------------------------------
    def download_torrent(self, torrent_id, artist_name, release_name, release_type_str, year, format_, media):
        self.logger.info(f"Downloading torrent ID {torrent_id} for '{release_name}' ({release_type_str}) by '{artist_name}'.")
        if torrent_id in self.already_failed_downloads:
            self.logger.info(f"Torrent ID {torrent_id} already failed a 4-times download error previously, skipping.")
            return None
        try:
            save_path = self.artist_torrents_download_path
            if not os.path.exists(save_path):
                os.makedirs(save_path)
                self.logger.debug(f"Created download directory: {save_path}")

            usetoken = 1 if self.tokens_available else 0
            download_params = {
                "action": "download",
                "id": torrent_id,
                "usetoken": usetoken
            }
            headers = {
                'Authorization': self.redacted_api_key
            }
            self.logger.debug(f"Download request: {download_params}")
            self.rate_limit()
            response = requests.get(self.redacted_api_url, params=download_params, headers=headers, stream=True)
            self.logger.debug(f"Download Response: {response.status_code}")
            content_type = response.headers.get('Content-Type')
            self.logger.debug(f"Content-Type: {content_type}")

            if response.status_code != 200:
                self.logger.error(f"Error downloading torrent ID {torrent_id}: {response.text}")
                # Attempt to parse error
                try:
                    error_data = response.json()
                    error_msg = error_data.get("error", "").lower()
                    if "freeleech tokens left" in error_msg:
                        self.logger.warning(f"No freeleech tokens available for torrent ID {torrent_id}. Disabling token usage.")
                        self.tokens_available = False
                        download_params["usetoken"] = 0
                        self.logger.debug(f"Retry download without tokens: {download_params}")
                        self.rate_limit()
                        response = requests.get(self.redacted_api_url, params=download_params, headers=headers, stream=True)
                        self.logger.debug(f"Retry Response: {response.status_code}")
                        content_type = response.headers.get('Content-Type')
                        self.logger.debug(f"Content-Type after retry: {content_type}")
                        if response.status_code != 200:
                            self.logger.error(f"Error downloading torrent ID {torrent_id} without tokens: {response.text}")
                            try:
                                error_data = response.json()
                                if "you have already downloaded this torrent file four times" in error_data.get("error", "").lower():
                                    self.logger.info(f"Marking torrent ID {torrent_id} as permanently failed (4-times error).")
                                    self.already_failed_downloads.add(torrent_id)
                            except:
                                pass
                            return None
                    else:
                        if "you have already downloaded this torrent file four times" in error_msg:
                            self.logger.info(f"Marking torrent ID {torrent_id} as permanently failed (4-times error).")
                            self.already_failed_downloads.add(torrent_id)
                except:
                    pass
                return None

            if not content_type or 'application/x-bittorrent' not in content_type.lower():
                self.logger.error(f"The downloaded content is not a torrent file for ID {torrent_id}. Content-Type: {content_type}")
                error_file = os.path.join(save_path, f"error_{torrent_id}.html")
                with open(error_file, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                self.logger.debug(f"Error content saved to: {error_file}")
                # Possibly parse for "4 times" again
                try:
                    error_data = response.json()
                    if "You have already downloaded this torrent file four times" in error_data.get("error", ""):
                        self.logger.info(f"Marking torrent ID {torrent_id} as permanently failed due to 4-times error.")
                        self.already_failed_downloads.add(torrent_id)
                except:
                    pass
                return None

            # Construct a filename
            filename_parts = [
                artist_name,
                "-",
                release_name,
                f"({release_type_str})",
                f"({year})",
                f"({format_})",
                f"({media})"
            ]
            filename = sanitize_filename(" ".join(filename_parts) + ".torrent")
            file_path = os.path.join(save_path, filename)

            # Save .torrent file
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            self.logger.info(f"Torrent downloaded: {file_path}")

            # Insert into downloaded_torrents
            self.cursor.execute('''
                INSERT OR IGNORE INTO downloaded_torrents (torrent_id, download_path, downloaded_at)
                VALUES (?, ?, ?)
            ''', (torrent_id, file_path, datetime.now()))
            self.conn.commit()
            self.logger.info(f"Torrent ID {torrent_id} recorded in 'downloaded_torrents'.")
            return file_path

        except Exception as e:
            self.logger.error(f"Error downloading torrent ID {torrent_id}: {e}")
            return None

    # -------------------------------------------------------------------------
    # process_artists_from_file
    # -------------------------------------------------------------------------
    def process_artists_from_file(self):
        if not os.path.exists("artists.txt"):
            self.logger.error("artists.txt not found. Please create a file with one artist name per line.")
            return
        snatched_torrent_ids = self.get_user_torrents_snatched()
        fulfilled_albums = self.get_fulfilled_albums()

        with open("artists.txt", "r", encoding="utf-8") as f:
            artists = [line.strip() for line in f if line.strip()]

        for artist_name in artists:
            self.logger.info(f"Processing artist: {artist_name}")
            browse_results = self.browse_artist_torrents(artist_name)
            if not browse_results:
                self.logger.info(f"No albums found for artist '{artist_name}'.")
                self.get_and_display_user_stats()
                continue

            for group_data in browse_results:
                group_id = group_data.get("groupId")
                release_name = group_data.get("groupName")
                year = group_data.get("groupYear")
                release_type_str = group_data.get("releaseType", "Unknown")

                if group_id in fulfilled_albums:
                    self.logger.info(
                        f"No new torrents for album '{release_name}' "
                        f"(GroupID {group_id}, Artist: {artist_name}). Already fulfilled."
                    )
                    continue

                if release_type_str not in self.selected_release_types:
                    self.logger.info(
                        f"Skipping album '{release_name}' (GroupID {group_id}, Artist: {artist_name}) "
                        f"not in selected release types."
                    )
                    continue

                torrents_list = group_data.get("torrents", [])
                filtered = []
                for tinfo in torrents_list:
                    tid = tinfo.get("torrentId")
                    if (tid in snatched_torrent_ids) and (not self.re_download_snatched):
                        self.logger.info(
                            f"Skipping torrent ID {tid} for album '{release_name}' (Artist: {artist_name}) "
                            "because it's already snatched and re_download_snatched=False."
                        )
                        continue
                    if self.should_skip_torrent(tinfo):
                        self.logger.info(
                            f"Skipping torrent ID {tid} for album '{release_name}' (Artist: {artist_name}) "
                            f"due to conditions (size/freeleech)."
                        )
                        continue
                    filtered.append(tinfo)

                if not filtered:
                    self.logger.info(
                        f"No suitable torrents for album '{release_name}' "
                        f"(GroupID {group_id}, Artist: {artist_name}) after filtering."
                    )
                    continue

                chosen = self.select_preferred_torrent(filtered)
                if not chosen:
                    self.logger.info(
                        f"No suitable torrents for album '{release_name}' "
                        f"(GroupID {group_id}, Artist: {artist_name}) after format/media preference."
                    )
                    continue

                chosen_tid = chosen.get("torrentId")
                chosen_format = chosen.get("format", "")
                chosen_media = chosen.get("media", "")
                chosen_size = chosen.get("size", 0)
                chosen_fileCount = chosen.get("fileCount", 0)
                chosen_release_type = release_type_str
                size_mb = self.bytes_to_mb(chosen_size)

                self.logger.info(
                    f"For album '{release_name}' (GroupID {group_id}), chosen torrent: "
                    f"ID {chosen_tid}, Media: {chosen_media}, Format: {chosen_format}, "
                    f"Tracks: {chosen_fileCount}, Size: {size_mb:.2f}MB, Type: {chosen_release_type}"
                )

                if self.download_torrents:
                    file_path = self.download_torrent(
                        chosen_tid,
                        artist_name,
                        release_name,
                        chosen_release_type,
                        year,
                        chosen_format,
                        chosen_media
                    )
                    if file_path:
                        try:
                            release_type_id = 0
                            for k, v in RELEASE_TYPE_MAP.items():
                                if v == chosen_release_type:
                                    release_type_id = k
                                    break
                            self.cursor.execute('''
                                INSERT OR IGNORE INTO snatched_torrents (
                                    torrent_id, group_id, release_name, release_type, artist_id,
                                    artist_name, year, format, media, size, seeders, leechers, snatched,
                                    time_added, download_url
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ''', (
                                chosen_tid,
                                group_id,
                                release_name,
                                release_type_id,
                                None,
                                artist_name,
                                year,
                                chosen_format,
                                chosen_media,
                                chosen_size,
                                chosen.get("seeders"),
                                chosen.get("leechers"),
                                chosen.get("snatches"),
                                chosen.get("time"),
                                f"{self.redacted_api_url}?action=download&id={chosen_tid}&usetoken={1 if self.tokens_available else 0}"
                            ))
                            self.conn.commit()
                            self.logger.info(f"Torrent ID {chosen_tid} inserted in DB.")
                        except Exception as e:
                            self.logger.error(f"Error inserting torrent ID {chosen_tid} into DB: {e}")

                        self.mark_album_fulfilled(group_id, chosen_format, chosen_media)
                    else:
                        self.logger.info(
                            f"Failed to download torrent ID {chosen_tid} for '{release_name}'."
                        )
                else:
                    self.logger.info(
                        f"Download is disabled. Would have chosen torrent ID {chosen_tid}."
                    )

            self.get_and_display_user_stats()

    # -------------------------------------------------------------------------
    # process_bookmarked_artists
    # -------------------------------------------------------------------------
    def process_bookmarked_artists(self):
        snatched_torrent_ids = self.get_user_torrents_snatched()
        fulfilled_albums = self.get_fulfilled_albums()
        all_bookmarks = self.get_all_bookmarked_artists()
        if not all_bookmarks:
            self.logger.info("No bookmarked artists found in DB.")
            return

        for (artist_id, artist_name) in all_bookmarks:
            if not artist_name:
                continue
            self.logger.info(f"Processing bookmarked artist: {artist_name} (ID {artist_id})")
            browse_results = self.browse_artist_torrents(artist_name)
            if not browse_results:
                self.logger.info(f"No albums found for artist '{artist_name}'.")
                self.get_and_display_user_stats()
                continue

            for group_data in browse_results:
                group_id = group_data.get("groupId")
                release_name = group_data.get("groupName")
                year = group_data.get("groupYear")
                release_type_str = group_data.get("releaseType", "Unknown")

                if group_id in fulfilled_albums:
                    self.logger.info(
                        f"No new torrents for album '{release_name}' "
                        f"(GroupID {group_id}, Artist: {artist_name}). Already fulfilled."
                    )
                    continue

                if release_type_str not in self.selected_release_types:
                    self.logger.info(
                        f"Skipping album '{release_name}' (GroupID {group_id}, Artist: {artist_name}) "
                        f"not matching selected release types."
                    )
                    continue

                torrents_list = group_data.get("torrents", [])
                filtered = []
                for tinfo in torrents_list:
                    tid = tinfo.get("torrentId")
                    if (tid in snatched_torrent_ids) and (not self.re_download_snatched):
                        self.logger.info(
                            f"Skipping torrent ID {tid} for album '{release_name}' "
                            f"(Artist: {artist_name}) because it's already snatched and re_download_snatched=False."
                        )
                        continue
                    if self.should_skip_torrent(tinfo):
                        self.logger.info(
                            f"Skipping torrent ID {tid} for album '{release_name}' "
                            f"(Artist: {artist_name}) due to conditions (size/freeleech)."
                        )
                        continue
                    filtered.append(tinfo)

                if not filtered:
                    self.logger.info(
                        f"No suitable torrents for album '{release_name}' "
                        f"(GroupID {group_id}, Artist: {artist_name}) after filtering."
                    )
                    continue

                chosen = self.select_preferred_torrent(filtered)
                if not chosen:
                    self.logger.info(
                        f"No suitable torrents for album '{release_name}' "
                        f"after format/media preference."
                    )
                    continue

                chosen_tid = chosen.get("torrentId")
                chosen_format = chosen.get("format", "")
                chosen_media = chosen.get("media", "")
                chosen_size = chosen.get("size", 0)
                chosen_release_type = release_type_str

                self.logger.info(
                    f"For album '{release_name}' (GroupID {group_id}), chosen torrent: "
                    f"ID {chosen_tid}, Media: {chosen_media}, Format: {chosen_format}..."
                )

                if self.download_torrents:
                    file_path = self.download_torrent(
                        chosen_tid,
                        artist_name,
                        release_name,
                        chosen_release_type,
                        year,
                        chosen_format,
                        chosen_media
                    )
                    if file_path:
                        try:
                            release_type_id = 0
                            for k,v in RELEASE_TYPE_MAP.items():
                                if v == chosen_release_type:
                                    release_type_id = k
                                    break
                            self.cursor.execute('''
                                INSERT OR IGNORE INTO snatched_torrents (
                                    torrent_id, group_id, release_name, release_type, artist_id,
                                    artist_name, year, format, media, size, seeders, leechers, snatched,
                                    time_added, download_url
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ''', (
                                chosen_tid,
                                group_id,
                                release_name,
                                release_type_id,
                                artist_id,
                                artist_name,
                                year,
                                chosen_format,
                                chosen_media,
                                chosen_size,
                                chosen.get("seeders"),
                                chosen.get("leechers"),
                                chosen.get("snatches"),
                                chosen.get("time"),
                                f"{self.redacted_api_url}?action=download&id={chosen_tid}&usetoken={1 if self.tokens_available else 0}"
                            ))
                            self.conn.commit()
                            self.logger.info(f"Torrent ID {chosen_tid} inserted in DB.")
                        except Exception as e:
                            self.logger.error(f"Error inserting torrent ID {chosen_tid} into DB: {e}")

                        self.mark_album_fulfilled(group_id, chosen_format, chosen_media)
                    else:
                        self.logger.info(
                            f"Failed to download torrent ID {chosen_tid} for '{release_name}'"
                        )
                else:
                    self.logger.info(
                        f"Download is disabled. Would have chosen torrent ID {chosen_tid}"
                    )

            self.get_and_display_user_stats()

    # -------------------------------------------------------------------------
    # Config menu for torrent download settings
    # -------------------------------------------------------------------------
    def configure_settings_interactively(self):
        while True:
            print("\n--- Configuration Menu ---")
            print("1. Select Formats (current: {})".format(", ".join(self.selected_formats)))
            print("2. Select Release Types (current: {})".format(", ".join(self.selected_release_types)))
            print("3. Set Media Order (current: {})".format(", ".join(self.media_preference)))
            print(f"4. Privilege releases with most tracks (current: {self.privilege_releases_with_most_tracks})")
            print(f"5. Snatch freeleech only (current: {self.snatch_freeleech_only})")
            print(f"6. Re-download snatched (current: {self.re_download_snatched})")
            print(f"7. Set max torrent size in MB (current: {self.max_torrent_size_mb}MB)")
            print(f"8. Set download path for torrents (current: {self.artist_torrents_download_path})")
            print("9. Back to main menu")

            choice = input("Select an option (1-9): ").strip()

            if choice == '1':
                print("Choose formats by their number, separated by comma:")
                for k,v in SUPPORTED_FORMATS.items():
                    print(f"{k}: {v}")
                sel = input("Enter choices: ")
                chosen_idx = [x.strip() for x in sel.split(",") if x.strip().isdigit()]
                self.selected_formats = [
                    SUPPORTED_FORMATS[int(x)]
                    for x in chosen_idx
                    if int(x) in SUPPORTED_FORMATS
                ]
            elif choice == '2':
                print("Available release types:")
                for k,v in RELEASE_TYPE_MAP.items():
                    print(f"{k}: {v}")
                sel = input("Enter IDs separated by comma: ")
                chosen_idx = [x.strip() for x in sel.split(",") if x.strip().isdigit()]
                chosen_labels = []
                for x in chosen_idx:
                    xint = int(x)
                    if xint in RELEASE_TYPE_MAP:
                        chosen_labels.append(RELEASE_TYPE_MAP[xint])
                self.selected_release_types = chosen_labels
            elif choice == '3':
                print("Select media order (enter indices in desired order):")
                for k,v in SUPPORTED_MEDIA.items():
                    print(f"{k}: {v}")
                sel = input("Enter order as indices separated by comma: ")
                chosen_idx = [x.strip() for x in sel.split(",") if x.strip().isdigit()]
                new_media_order = []
                for c in chosen_idx:
                    cint = int(c)
                    if cint in SUPPORTED_MEDIA and SUPPORTED_MEDIA[cint] not in new_media_order:
                        new_media_order.append(SUPPORTED_MEDIA[cint])
                if new_media_order:
                    self.media_preference = new_media_order
            elif choice == '4':
                self.privilege_releases_with_most_tracks = not self.privilege_releases_with_most_tracks
            elif choice == '5':
                self.snatch_freeleech_only = not self.snatch_freeleech_only
            elif choice == '6':
                self.re_download_snatched = not self.re_download_snatched
            elif choice == '7':
                val = input("Enter max torrent size in MB: ").strip()
                if val.isdigit():
                    self.max_torrent_size_mb = int(val)
            elif choice == '8':
                val = input("Enter download path: ").strip()
                if val:
                    self.artist_torrents_download_path = val
            elif choice == '9':
                break
            else:
                print("Invalid choice. Try again.")

    # -------------------------------------------------------------------------
    # Main Menu
    # -------------------------------------------------------------------------
    def main_menu(self):
        while True:
            print("\n--- Main Menu ---")
            print("1. Process artists from 'artists.txt' to find and download missing torrents")
            print("2. Process bookmarked artists to find and download missing torrents")
            print("3. Show bookmarked artists (from DB)")
            print("4. Configure torrent download settings")
            print("5. Quit")

            choice = input("Select an option (1-5): ").strip()

            if choice == '1':
                self.process_artists_from_file()
            elif choice == '2':
                self.process_bookmarked_artists()
            elif choice == '3':
                self.display_bookmarked_artists()
            elif choice == '4':
                self.configure_settings_interactively()
            elif choice == '5':
                self.logger.info("Exiting script.")
                self.close()
                sys.exit(0)
            else:
                print("Invalid option.")

    def display_bookmarked_artists(self):
        artists = self.get_all_bookmarked_artists()
        if not artists:
            print("No bookmarked artists found.")
            return
        print("\n--- Bookmarked Artists ---")
        for a in artists:
            print(f"ID: {a[0]} | Name: {a[1]}")

    def close(self):
        self.conn.close()
        self.logger.info("Database connection closed.")

# -------------------------------------------------------------------------
# FLASK + Automatic self-signed certificate
# -------------------------------------------------------------------------
app = Flask(__name__)
snatcher_instance = None

def generate_self_signed_cert(certfile, keyfile):
    """
    Use 'openssl' to generate a self-signed certificate with minimal user input,
    valid for a very long period (e.g. 999999 days).
    All fields are set to 'redsnatcher'.
    """
    command = [
        "openssl", "req",
        "-x509",
        "-nodes",
        "-newkey", "rsa:4096",
        "-days", "999999",
        "-subj", "/C=XX/ST=redsnatcher/L=redsnatcher/O=redsnatcher/OU=redsnatcher/CN=redsnatcher",
        "-keyout", keyfile,
        "-out", certfile
    ]
    print(f"[Auto-Cert] Generating self-signed certificate: {certfile}, key: {keyfile}")
    subprocess.check_call(command)

@app.route('/api/redsnatcher', methods=['POST'])
def handle_redsnatcher_api():
    global snatcher_instance
    if snatcher_instance is None:
        return jsonify({"status": "error", "message": "No RedSnatcher instance available"}), 500

    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({"status": "error", "message": "No JSON payload provided"}), 400

    command = data.get("command")
    artist = data.get("artist", "")

    if command == "process_artists_file":
        snatcher_instance.logger.info("[Daemon] Received command: process_artists_file")
        snatcher_instance.process_artists_from_file()
        return jsonify({"status": "ok", "command": command, "info": "Started processing artists.txt"})
    elif command == "process_bookmarked":
        snatcher_instance.logger.info("[Daemon] Received command: process_bookmarked")
        snatcher_instance.process_bookmarked_artists()
        return jsonify({"status": "ok", "command": command, "info": "Started processing bookmarked artists"})
    elif command == "process_current_artist":
        snatcher_instance.logger.info("[Daemon] Received command: process_current_artist")
        if not artist:
            return jsonify({"status": "error", "message": "No artist provided"}), 400
        snatcher_instance.process_single_artist(artist)
        return jsonify({"status": "ok", "command": command, "info": f"Started processing artist: {artist}"})
    else:
        return jsonify({"status": "error", "message": f"Unknown command: {command}"}), 400

def run_daemon_mode(config_path='config.json'):
    global snatcher_instance
    snatcher_instance = RedSnatcher(config_path=config_path)
    host = snatcher_instance.daemon_host
    port = snatcher_instance.daemon_port

    certfile = "redsnatcher.crt"
    keyfile = "redsnatcher.key"

    # If either file doesn't exist, generate a new self-signed cert
    if not os.path.exists(certfile) or not os.path.exists(keyfile):
        generate_self_signed_cert(certfile, keyfile)

    snatcher_instance.logger.info(
        f"Starting Flask daemon (HTTPS) on {host}:{port} with cert={certfile} and key={keyfile}"
    )
    app.run(host=host, port=port, debug=False, ssl_context=(certfile, keyfile))

def main():
    parser = argparse.ArgumentParser(description="RedSnatcher - Snatch torrents from Redacted based on filters")
    parser.add_argument('--config', type=str, default='config.json', help="Path to config.json")
    parser.add_argument('--daemon', action='store_true', help="Run in daemon (Flask) mode")
    args = parser.parse_args()

    if args.daemon:
        run_daemon_mode(config_path=args.config)
    else:
        try:
            snatcher = RedSnatcher(config_path=args.config)
            # Start interactive menu after full initialization.
            snatcher.main_menu()
        except KeyboardInterrupt:
            snatcher.close()
            print("\nScript interrupted by user.")
        except Exception as e:
            print(f"Critical error: {e}")

if __name__ == "__main__":
    main()
