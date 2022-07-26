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

def parse_commentforest(sub_info, comment_parsing_ts, subreddit, db_conn,
                        comments_tname):

    sub_comment_forest = sub_info[0]
    sub_id = sub_info[1]

    # REPLACING "more comment" OBJECTS
    while True:
        try:
            sub_comment_forest.replace_more(limit = 15, threshold = 3)
            break
        except Exception as e:
            message = "When expanding comments for submission {} in subreddit {}, exception {} occurred".format(sub_id, subreddit, e)
            print(message)

    # CHECKING FOR SUBMISSION IDS ALREADY ENTERED IN THE DATABASE
    validation_query = """SELECT comment_id 
                            FROM {}
                           WHERE sub_id = \"{}\"""".format(comments_tname, sub_id)
    existing_ids = pd.read_sql(validation_query, db_conn)
    # print(list(existing_ids["comment_id"]))
    
    # GETTING COMMENT INFORMATION
    sub_comments = [[comment.link_id, comment.id, comment.body]
                        for comment in sub_comment_forest]

    # FILTERING COMMENTS FOR COMMENTS NOT ALREADY ENTERED
    ncomm_before_dropping_existing = len(sub_comments)
    sub_comments = [comm_info for comm_info in sub_comments
                        if comm_info[1] not in list(existing_ids["comment_id"])]
    
    if len(sub_comments) == 0:
        # print("No new comments for submission {} in subreddit {}".format(sub_id, subreddit))
        return
    
    ncomm_after_dropping_existing = len(sub_comments)
    diff_ncomm = ncomm_before_dropping_existing - ncomm_after_dropping_existing
    # print("Number of comments already in table for submission {} is {}".format(sub_id, diff_ncomm))

    # CONVERTING TO DATAFRAME
    sub_comments = [[comm_info[0], comm_info[1], comm_info[2]] 
                        for comm_info in sub_comments]

    sub_comments_df = pd.DataFrame(sub_comments, 
                                   columns = ["sub_id", "comment_id", "comment_body"])
    sub_comments_df["comment_polled_ts"] = comment_parsing_ts

    return sub_comments_df    

def poll_comments(rdt_scraper, db_connection, admin_recs_tname, comment_tname, 
                  subreddit_to_poll):

    poll_datetime = dt.now()

    # GETTING SUBMISSIONS TO GET COMMENTS FOR
    admin_ids_qry = """SELECT sub_id FROM submission_admin 
                        WHERE subreddit =\"{}\";""".format(subreddit_to_poll)
    admin_ids_table = pd.read_sql_query(admin_ids_qry, db_connection)

    # GETTING COMMENTS FROM THE SELECTED SUBMISSIONS
    subs_to_extr_comments = rdt_scraper.info(fullnames = list(admin_ids_table["sub_id"]))
    sub_comments = [[sub.comments, sub.fullname] for sub in subs_to_extr_comments]

    # PARSING COMMENTFORESTS TO GET DF OF COMMENTS
    comment_info_df_list = [parse_commentforest(sub_info, poll_datetime, subreddit_to_poll, 
                                                db_connection, "submission_comments") 
                                for sub_info in sub_comments]
    comment_info_df_list = [comment_info_df for comment_info_df in comment_info_df_list
                                if comment_info_df is not None]
    if len(comment_info_df_list) == 0:
        message = """No new comments found for submissions in {} at 
                     time {}""".format(subreddit_to_poll, poll_datetime)
        print(message)
        return None
    
    comment_info = pd.concat(comment_info_df_list)
    comment_info.to_sql(comment_tname, db_connection, if_exists="append", index=False)


