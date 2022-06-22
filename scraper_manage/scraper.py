from asyncore import poll
import praw
import pandas as pd
from datetime import datetime as dt

def init_scraper(client_id, client_secret, user_agent):

    scraper_obj = praw.Reddit(client_id=client_id,
                              client_secret=client_secret,
                              user_agent=user_agent)
    
    return scraper_obj

def proc_new_submissions(subs, existing_sub_ids, subs_subreddit, db_connection, 
                         admin_recs_tname, sub_info_tname, poll_datetime):

    # GETTING SUBMISSION INFO
    new_sub_info = [[sub.fullname, sub.score, sub.upvote_ratio, sub.num_comments, 
                     sub.locked, sub.url] for sub in subs]
    new_sub_info_df = pd.DataFrame(new_sub_info, 
                                   columns = ["sub_id", "num_ups", "up_ratio", 
                                              "num_comms", "is_sub_locked", "sub_url"])
    new_sub_info_df["subreddit"] = subs_subreddit
    new_sub_info_df["ts_first_polled"] = poll_datetime
    new_sub_info_df["ts_last_polled"] = poll_datetime

    # DROPPING SUBMISSIONS WHICH ARE ALREADY ENTERED AND ARE REPOLLED
    print(new_sub_info_df["sub_id"])
    new_sub_info_df = new_sub_info_df.loc[~new_sub_info_df["sub_id"].isin(existing_sub_ids), :]
    if len(new_sub_info_df.index.values) == 0:
        print("No new submissions were found at poll {}".format(poll_datetime))
        return None

    # CREATING THE TABLES TO WRITE
    new_sub_admininfo = new_sub_info_df[["sub_id", "subreddit", "ts_first_polled", "ts_last_polled", "sub_url"]]
    new_sub_info = new_sub_info_df[["sub_id", "ts_last_polled", "num_ups", "up_ratio", "num_comms", "is_sub_locked"]]

    # WRITING TABLES
    new_sub_admininfo.to_sql(name=admin_recs_tname, con=db_connection, 
                             if_exists="append", index=False)
    new_sub_info.to_sql(name=sub_info_tname, con=db_connection, 
                        if_exists="append", index=False)

    return None

def proc_existing_submissions(subreddit_to_poll, db_connection, rdt_scraper, 
                              admin_recs_tname, sub_info_tname, poll_datetime):

    # GETTING EXISTING SUBMISSIONS ADMIN INFO 
    admin_recs_qry = """SELECT * FROM {} 
                        WHERE subreddit =\"{}\" """.format(admin_recs_tname, subreddit_to_poll)
    admin_recs_table = pd.read_sql_query(admin_recs_qry, db_connection)
    sub_ids = admin_recs_table["sub_id"]

    # POLLING THE EXISTING SUBMISSIONS
    sub_list = rdt_scraper.info(list(sub_ids)) 
    sub_info = [[sub.fullname, sub.score, sub.upvote_ratio, sub.num_comments, 
                 sub.locked] for sub in sub_list]
    sub_info_table = pd.DataFrame(sub_info, columns = ["sub_id", "num_ups", "up_ratio", "num_comms", "is_sub_locked"])
    sub_info_table["ts_last_polled"] = poll_datetime

    # UPDATING THE CORRESPONDING ADMIN RECORD
    subs_to_update = list(sub_ids)
    subs_to_update = ["\"{}\"".format(id) for id in subs_to_update]
    subs_to_update = ",".join(subs_to_update)
    update_sql = """UPDATE {}
                       SET ts_last_polled = \"{}\"
                     WHERE sub_id in ({});""".format(admin_recs_tname, poll_datetime,
                                                    subs_to_update)
    print(update_sql)
    curs = db_connection.cursor()
    curs.execute(update_sql)
    db_connection.commit()
    
    # INSERTING THE NEW DATA RECORDS
    sub_info_table.to_sql(name=sub_info_tname, con=db_connection, if_exists="append",
                          index=False)

    return sub_ids

def poll_subreddit(subreddit_to_poll, rdt_scraper, n_new_submissions,
                   db_connection, admin_recs_tname, sub_info_tname):

    poll_time = str(dt.now())

    # POLLING EXISTING SUBMISSIONS IN SELECTED SUBREDDIT
    existing_sub_ids = proc_existing_submissions(subreddit_to_poll=subreddit_to_poll,
                                                 db_connection=db_connection,
                                                 rdt_scraper=rdt_scraper,
                                                 admin_recs_tname=admin_recs_tname, 
                                                 sub_info_tname=sub_info_tname, 
                                                 poll_datetime=poll_time)

    # POLLING NEW SUBMISSIONS IN SELECTED SUBREDDIT
    new_posts = rdt_scraper.subreddit(subreddit_to_poll).new(limit = n_new_submissions)

    proc_new_submissions(subs=new_posts, existing_sub_ids=existing_sub_ids, 
                         subs_subreddit=subreddit_to_poll, db_connection=db_connection, 
                         admin_recs_tname=admin_recs_tname, sub_info_tname=sub_info_tname,
                         poll_datetime=poll_time)

    return None