import simcity_client
import argparse
import time

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Make old locked tokens available for processing again (default: all)")
    parser.add_argument('-D', '--days', type=int, help="number of days ago the token was locked", default=0)
    parser.add_argument('-H', '--hours', type=int, help="number of hours ago the token was locked", default=0)
    parser.add_argument('-S', '--seconds', type=int, help="number of seconds ago the token was locked", default=0)
    parser.add_argument('-V', '--view', choices=['locked', 'error'], default='locked', help="view to scrub")
    args = parser.parse_args()
    
    arg_t = 60*((24*args.days) + args.hours) + args.seconds
    min_t = int( time.time() ) - arg_t
    
    db = simcity_client.init()['database']

    update = []
    for row in db.view(args.view):
        if arg_t <= 0 or row['value']['lock'] < min_t:
            token = db.get_token(row['key'])
            token.scrub()
            update.append(token)
    
    if len(update) > 0:
        db.save_tokens(update)
        print "Scrubbed", len(update), "token(s)"
    else:
        print "No scrubbing required"
