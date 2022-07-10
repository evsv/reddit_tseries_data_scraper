from asyncore import poll
import praw
import pandas as pd
import datetime
from datetime import datetime as dt
import sqlite3

def init_scraper(client_id, client_secret, user_agent):

    scraper_obj = praw.Reddit(client_id=client_id,
                              client_secret=client_secret,
                              user_agent=user_agent)
    
    return scraper_obj

def validate_data_entry(new_submission_admin, db_connection, admin_recs_tname, 
                        subreddit):

    # GETTING EXISTING SUBMISSIONS
    admin_recs_query = """SELECT * FROM {} 
                          WHERE subreddit =\"{}\"""".format(admin_recs_tname, subreddit)
    admin_recs_table = pd.read_sql_query(admin_recs_query, db_connection)

    # GETTING SUBMISSIONS WITH COMMON IDS
    admin_recs_ids = admin_recs_table.sub_id
    new_submission_ids = new_submission_admin.sub_id
    common_submission_ids = [id for id in new_submission_ids 
                                if id in list(admin_recs_ids)]
    columns_to_verify = ["sub_id", "sub_title", "ts_sub_created"]

    print("Existing admin records with conflicting IDs:")
    print(admin_recs_table.loc[admin_recs_table.sub_id.isin(common_submission_ids), columns_to_verify])
    print("New admin records with conflicting IDs:")
    print(new_submission_admin.loc[new_submission_admin.sub_id.isin(common_submission_ids), columns_to_verify])

    return common_submission_ids

def proc_new_submissions(subs, existing_sub_ids, subs_subreddit, title_keywords, 
                         db_connection, admin_recs_tname, sub_info_tname, 
                         poll_datetime):

    # GETTING SUBMISSION INFO
    new_sub_info = [[sub.fullname, sub.score, sub.upvote_ratio, sub.num_comments, 
                     sub.locked, sub.title, sub.url, sub.created_utc] for sub in subs]
    new_sub_info_df = pd.DataFrame(new_sub_info, 
                                   columns = ["sub_id", "num_ups", "up_ratio", 
                                              "num_comms", "is_sub_locked", "sub_title",
                                              "sub_url", "ts_sub_created"])
    new_sub_info_df["subreddit"] = subs_subreddit
    new_sub_info_df["ts_first_polled"] = poll_datetime
    new_sub_info_df["ts_last_polled"] = poll_datetime

    # FILTERING FOR SUBMISSIONS CONTAINING KEYWORDS
    strings_to_search = "|".join(title_keywords)
    new_sub_info_df = new_sub_info_df.loc[new_sub_info_df.sub_title.str.contains(strings_to_search, case=False), :]

    # DROPPING SUBMISSIONS WHICH ARE ALREADY ENTERED AND ARE REPOLLED

    new_sub_info_df = new_sub_info_df.loc[~new_sub_info_df["sub_id"].isin(existing_sub_ids), :]
    if len(new_sub_info_df.index.values) == 0:
        print("No new submissions were found at poll {} in subreddit {}".format(poll_datetime, subs_subreddit))
        return None
    # print(new_sub_info_df)
    # CREATING THE TABLES TO WRITE
    new_sub_admininfo = new_sub_info_df[["sub_id", "subreddit", "ts_first_polled", "ts_last_polled", "sub_title", "sub_url", "ts_sub_created"]]
    new_sub_info = new_sub_info_df[["sub_id", "ts_last_polled", "num_ups", "up_ratio", "num_comms", "is_sub_locked"]]

    # WRITING TABLES
    try:
        new_sub_admininfo.to_sql(name=admin_recs_tname, con=db_connection, 
                                 if_exists="append", index=False)
        new_sub_info.to_sql(name=sub_info_tname, con=db_connection, 
                            if_exists="append", index=False)
    except sqlite3.IntegrityError as ex:
        message = """SQL Integrity Error {0} when processing new submissions. Arguments:\n{1!r}"""
        
        if ex.args[0] == "UNIQUE constraint failed: submission_admin.sub_id":
            conflicting_ids = validate_data_entry(new_submission_admin=new_sub_admininfo, 
                                                  db_connection=db_connection, 
                                                  admin_recs_tname=admin_recs_tname, 
                                                  subreddit=subs_subreddit)
            
            new_sub_admininfo_wo_conflict = new_sub_admininfo.loc[~new_sub_admininfo.sub_id.isin(conflicting_ids), :]
            new_sub_info_wo_conflict = new_sub_info.loc[~new_sub_info.sub_id.isin(conflicting_ids), :]
            
            if len(new_sub_admininfo_wo_conflict.index.values) == 0:
                print("No new submissions after correcting for submission ID conflicts at {} in subreddit {}".format(poll_datetime, subs_subreddit))
                return None
            
            new_sub_admininfo_wo_conflict.to_sql(name=admin_recs_tname, con=db_connection, 
                                                 if_exists="append", index=False)
            new_sub_info_wo_conflict.to_sql(name=sub_info_tname, con=db_connection, 
                                            if_exists="append", index=False)

    return None

def proc_existing_submissions(subreddit_to_poll, db_connection, rdt_scraper, 
                              admin_recs_tname, sub_info_tname, poll_datetime,
                              poll_until):

    # GETTING EXISTING SUBMISSIONS ADMIN INFO 
    admin_recs_qry = """SELECT * FROM {} 
                        WHERE subreddit =\"{}\" AND 
                              ts_first_polled >= \"{}\";""".format(admin_recs_tname, subreddit_to_poll, poll_until)
    admin_recs_table = pd.read_sql_query(admin_recs_qry, db_connection)
    if len(admin_recs_table.index.values) == 0:
        print("No valid existing records in subreddit {} at poll time {}".format(subreddit_to_poll, poll_datetime))

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

    curs = db_connection.cursor()
    curs.execute(update_sql)
    db_connection.commit()
    
    # INSERTING THE NEW DATA RECORDS
    sub_info_table.to_sql(name=sub_info_tname, con=db_connection, if_exists="append",
                          index=False)

    return sub_ids

def poll_subreddit(subreddit_to_poll, rdt_scraper, n_new_submissions,
                   title_keywords, db_connection, admin_recs_tname, 
                   sub_info_tname, ndays_back_to_poll):

    poll_time = str(dt.now())
    poll_until = str(dt.now()-datetime.timedelta(days=ndays_back_to_poll))

    # POLLING EXISTING SUBMISSIONS IN SELECTED SUBREDDIT
    existing_sub_ids = proc_existing_submissions(subreddit_to_poll=subreddit_to_poll,
                                                 db_connection=db_connection,
                                                 rdt_scraper=rdt_scraper,
                                                 admin_recs_tname=admin_recs_tname, 
                                                 sub_info_tname=sub_info_tname, 
                                                 poll_datetime=poll_time,
                                                 poll_until=poll_until)

    # POLLING NEW SUBMISSIONS IN SELECTED SUBREDDIT
    new_posts = rdt_scraper.subreddit(subreddit_to_poll).new(limit = n_new_submissions)

    proc_new_submissions(subs=new_posts, existing_sub_ids=existing_sub_ids, 
                         subs_subreddit=subreddit_to_poll, title_keywords=title_keywords,
                         db_connection=db_connection, admin_recs_tname=admin_recs_tname, 
                         sub_info_tname=sub_info_tname, poll_datetime=poll_time)

    return None