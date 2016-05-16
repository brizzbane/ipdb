from ipdb import build_db, IpAsnRangeDoc

build_db()

amazonranges = IpAsnRangeDoc().search().query('match', owner='amazon.com').execute()