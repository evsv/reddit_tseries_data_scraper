import json
import scraper_manage.scraper as scrm
import pandas as pd
import sqlite3 as sql
from datetime import datetime as dt
import time

if __name__ == "__main__":

    reddit_info_config_fpath = "scraper_manage/reddit_config.json"
    auth_config_fpath = "scraper_manage/auth.json"

    with open(reddit_info_config_fpath, "r") as f:
        reddit_info_config = json.load(f)
        
    with open(auth_config_fpath) as f:
        auth_config = json.load(f)

    rdt_scraper = scrm.init_scraper(client_id=auth_config["client_id"],
                                    client_secret=auth_config["client_secret"],
                                    user_agent=auth_config["user_agent"])

    db_conn = sql.connect("reddit_db/reddit_sub_info.db")

    iter = 0
    while True:

        # Polling for submissions
        for sub in reddit_info_config["subreddits"]:

            scrm.poll_subreddit(subreddit_to_poll=sub, 
                                rdt_scraper=rdt_scraper, 
                                title_keywords=reddit_info_config["keywords"],
                                n_new_submissions=500,
                                db_connection=db_conn, 
                                admin_recs_tname="submission_admin", 
                                sub_info_tname="submission_data",
                                ndays_back_to_poll=reddit_info_config["poll_limit_days"])

        
        # Polling for comments - we poll every 1 hour, since comments 
        # are relatively expensive to poll
        iter = iter+1
        if iter == 4:
            scrm.poll_comments(rdt_scraper=rdt_scraper, 
                                db_connection=db_conn, 
                                admin_recs_tname="submission_admin", 
                                comment_tname="submission_comments", 
                                subreddit_to_poll=sub)
            iter = 0

        print("TICK: {}".format(dt.now()))
        time.sleep(15*60)

    db_conn.close()
