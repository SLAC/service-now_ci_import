from slac_ci.datasources.oracle import Oracle

import logging
LOG = logging.getLogger()


def match_user_by_name( lastname, firstname, users={} ):
    if id in users:
        return users[id]['lastname'], users[id]['firstname']
    return None, None


class Users( Oracle ):

    by_id = {}
    by_name = {}
    by_username = {}

    def get_users( self ):
        """ query sid for user info, but merge with data from res """

        # Useracct_admin.but.BUT_LDT='win' OR Useracct_admin.but.BUT_LDT='unix'
        res = {}
        for d in self.query( """
        select 
            BUT_LDT as account, 
            BUT_LID as username, 
            BUT_SID as id, 
            Useracct_admin.but.BUT_UUID as res_id, 
            EMAIL_ADDRESS as email,
            useracct_admin.mail_master.reverse_flag as flag
        from 
            Useracct_admin.but  
        LEFT JOIN useracct_admin.mail_master ON useracct_admin.mail_master.but_uuid=Useracct_admin.but.but_uuid 
        where 
            Useracct_admin.but.BUT_LDT='mail'
            AND Useracct_admin.mail_master.reverse_flag IN ( 'X', 'M' )
        """ ):
            # validate against email
            u,_,domain = d['email'].partition('@')
            # LOG.error("1) %s\t%s\t%s email %s" % (d['username'],u,d['id'],d['email']))
            if u.lower() == d['username'].lower() or '.' in d['username']:
                # LOG.error("%s \t-> %s \t %s" % (u,d['username'],d['id']))
                # swap user/u if dot in username
                if '.' in d['username']:
                    #LOG.error("SWAP:%s %s: %s" % (u, d['username'],d) )
                    t = d['username'] 
                    d['username'] = u
                    u = t
                i = d['id']
                if not i in res:
                    res[i] = []
                # if not d['username'] in res[i]:
                #     res[i][d['username']] = []
                # res[i][d['username']].append( d['email'] )
                d['email'] = d['email'].lower().replace( '@exchange.','@' ).replace( '@mailbox.', '@')
                res[i].append( d )
            # elif '.' in u.lower():
            #     i = d['id']
            
        for d in self.query( """
        select
            BUT_LDT as account,
            BUT_LID as username,
            BUT_SID as id
        from
            Useracct_admin.but
        where
            Useracct_admin.but.BUT_LDT='win' OR Useracct_admin.but.BUT_LDT='unix'
        """ ):
            i = d['id']
            if i in res:
                res[i].append( d )
 

        self.by_id = {}
        self.by_name = {}
        self.by_username = {}
        for d in self.query("""
        select
          p.key AS id,
          p.lname as lastname,
          p.fname as firstname,
          p.ext as telephone,
          o.description department,
          d.description directorate,
          p.status as status
        from
           persons.person p,
           sid.organizations o,
           sid.organizations d
        where
            P.DEPT_ID = O.ORG_ID(+) 
            AND O.DIRECTORATE_CODE = D.DIRECTORATE_CODE 
            AND D.ORG_LEVEL = 2
        """):
            try:
                if d['id'] in res:
                    # d['username'] = res[d['id']]
                    # hmm.. assume first?
                    #LOG.error(">> %s" % (res[d['id']],))
                    this = res[d['id']].pop(0)
                    d['username'] = this['username']
                    d['email'] = this['email']
                    while '.' in d['username'] or '_' in d['username']:
                        this = res[d['id']].pop(0)
                        d['username'] = this['username']
                    #LOG.error("  >> %s" % (d,))

                self.by_id[d['id']] = d
                # print "2) %s" % (d,)
                n = "%s, %s"% ( d['lastname'], d['firstname'] )
                self.by_name[n.upper()] = d
                if 'username' in d:
                    self.by_username[d['username']] = d
            except Exception, e:
                LOG.debug("no username: %s in %s" % (e,d))
                # LOG.error("could not determine %s" % (d,))

        return True
    

    