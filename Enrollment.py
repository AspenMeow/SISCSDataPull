import numpy as np
import pandas as pd
import connectDB as ct


class stdntterm:
    term = None
    df = pd.DataFrame()

    #default constructor
    def __init__(self, term=None):
        self.term = term
    #overload default constructor with current term
    @classmethod
    def withCurrentTerm(cls):
        tst = pd.read_sql(
            "select max(strm) as term from siscs.r_term_rv where sysdate between term_begin_dt and term_end_dt ",
            ct.EDW)['term']
        return cls(tst.tolist()[0])

    #overload default constructor with any selected term
    @classmethod
    def withSelectTerm(cls, term):
        return cls(term)

    #function to pull student enrollment and assessed credit hours
    #plvt enrollment by person acad career
    def termenrl(self):
        df = pd.read_sql("select * from (select a.emplid,a.strm,a.term_descrshort,  a.acad_career, a.degree_career,a.enrollment_status, \
         a.residency, a.admission_res, a.tuition_res,fin_aid_fed_res,\
         a.acad_level_bot, a.TOT_PROJ_UNITS, x.XLATSHORTNAME as acad_level_bot_short_descr, x.XLATLONGNAME as acad_level_bot_long_descr,  a.acad_plan, c.acad_plan_descr,\
         c.acad_plan_u1, acad_plan_u1_descrformal, acad_plan_mau, acad_plan_mau_descrformal, b.citizenship_status, b.citizenship_descrshort, b.sex, \
         b.IPEDS_RACE_ETHNICITY,b.res_country, b.res_state, b.res_county,b.res_city,count(*) over (partition by a.emplid, a.acad_career, a.institution order by x.effdt desc ) as cnt \
        from  siscs.r_studentterm_rv a  inner join siscs.r_stdntbiodemo_rv b on a.emplid =b.emplid \
        inner join siscs.r_acadplan_Rv c on a.acad_plan=c.acad_plan  and a.institution=c.institution \
         left join siscs.t_xlatitem_v x on a.acad_level_bot=x.fieldvalue and x.fieldname ='ACADEMIC_LEVEL' and x.effdt <=sysdate \
       where a.enrollment_status in ('C','E','W') and a.primary_car_flag='Y' and a.strm="+self.term+") \
        where cnt=1 or cnt is null   ", ct.EDW)
        return df

    #pcrs pull course enroll
    def crsenrl(self):
        df = pd.read_sql("select * from \
                (select sc.emplid, sc.strm,sc.class_nbr,sc.subject,sc.acad_career,sc.crse_career, sc.crse_code, sc.class_section ,sc.UNT_BILLING as asessed_credit,\
                 sc.CRSE_GRADE_OFF,cc.crse_effdt,crse_effdt_end, crse_eff_Status,sc.repeat_code, cs.section_id, cc.course_title_long, cc.acad_org_mau,\
                  cc.acad_org_mau_descrformal,cc.acad_org_u1, cc.acad_org_u1_descrformal,cs.SESSION_CODE,cs.LOCATION, cs.class_type,cs.class_type_xlatlongname,\
                   ts.sess_begin_dt, ts.sess_end_dt,count(*)over (partition by sc.emplid, sc.strm,sc.class_nbr,sc.subject,sc.acad_career, sc.crse_code, sc.class_section order by cc.crse_effdt desc  ) as cnt \
                    from siscs.r_studentclass_Rv sc inner join  siscs.R_COURSECATALOG_RV cc on sc.crse_id =cc.crse_id and sc.subject= cc.subject \
                    and sc.crse_code =cc.crse_code    inner join (select acad_career, strm, lead(term_begin_dt-1) over (partition by institution, acad_career order by strm) as next_term_begin_dt1 \
                from siscs.r_term_rv ) t  on sc.strm =t.strm and sc.acad_career =t.acad_career inner join siscs.R_CLASSSCHEDULE_RV cs \
                on sc.strm=cs.strm and sc.class_nbr=cs.class_nbr and sc.subject=cs.subject and sc.crse_code =cs.crse_code and sc.class_section =cs.class_section \
            inner join siscs.r_termsession_rv ts on sc.strm =ts.strm and sc.acad_career=ts.acad_career and cs.session_code= ts.session_code \
            where sc.strm="+ self.term+" and  cc.crse_effdt <=next_term_begin_dt1 and class_stat='A') where cnt=1", ct.EDW)

        return df

    def enrlsch(self):
        df = self.termenrl().join(self.crsenrl().set_index(['emplid','strm']), on=['emplid','strm'], rsuffix='_crs')
        return df

    def race(self):
        sql ="select re.emplid,re.HISP_LATINO ,re.country, re.citizenship_status,re.setid,re.ethnic_group, re.xlatlongname ,re.descr as citizenship_descrshort \
        from ( \
         select e.emplid,  c.ethnic_grp_cd,c.hisp_latino,e.country, e.citizenship_status,t.setid,t.ethnic_group, s.descr, \
         row_number() over( partition by e.emplid, e.country, c.ethnic_grp_cd order by t.effdt desc ) as n, t_xlattable_vw.xlatlongname \
          from   siscs.p_citizenship_av e \
        left join siscs.s_citizen_sts_tbl_AV s \
        on e.country=s.country \
        and e.citizenship_status =s.citizenship_status \
        and s.edw_curr_ind='Y' and s.edw_actv_ind='Y' \
        left join siscs.p_ethnicity_dtl_av  c\
        on  e.emplid = c.emplid and c.edw_curr_ind='Y' and c.edw_actv_ind='Y' \
        left join  siscs.s_ethnic_grp_tbl_av  t \
        on c.ethnic_grp_cd= t.ethnic_grp_cd \
        and t.edw_curr_ind='Y' and t.edw_actv_ind='Y' \
        and t.effdt <= sysdate \
        left  join (select * from siscs.t_xlattable_vw_av where edw_actv_ind='Y' and edw_curr_ind='Y')  t_xlattable_vw on t_xlattable_vw.fieldname = 'ETHNIC_GROUP'	 \
        and t_xlattable_vw.fieldvalue = t.ethnic_group	\
        and t_xlattable_vw.eff_status = 'A' \
        where e.edw_curr_ind='Y' and e.edw_actv_ind='Y' and \
        e.emplid  in (select emplid from siscs.p_stdnt_car_term_av where edw_curr_ind='Y' and edw_actv_ind='Y' and strm=" +self.term+" )	) re \
        where n=1 and re.country ='USA'"
        df = pd.read_sql(sql,ct.EDW)
        return df

    def raceagg(self):
        race = self.race()
        race['Intl'] = np.where((race['citizenship_status'] == '4') & (race['country'] == 'USA'), 'Y', 'N')
        raceagg = race.groupby(['emplid', 'citizenship_status', 'citizenship_descrshort', 'country'])[
            'hisp_latino', 'Intl'].max()

        nonhis = race.query("hisp_latino=='N' & setid=='USA'").loc[:, ['emplid', 'ethnic_group']].drop_duplicates()
        nonhis = nonhis.groupby('emplid').size().reset_index()
        nonhis.columns = ['emplid', 'REcnt']

        raceagg = raceagg.join(nonhis.set_index('emplid'))
        onerace = raceagg.query('REcnt==1')

        onerace = onerace.reset_index().join(
            race.query("hisp_latino=='N' & setid=='USA'").set_index('emplid').loc[:, 'xlatlongname'],   on='emplid').drop_duplicates()


        raceagg = raceagg.reset_index().join(onerace.set_index('emplid').loc[:, 'xlatlongname'], on='emplid')

        raceagg['ipeds_race_ethnicity'] = np.where((raceagg['citizenship_status'] == '4') & (raceagg['country'] == 'USA'),
                                      'International', np.where(raceagg['hisp_latino'] == 'Y', 'Hispanic/Latino',
                                               np.where(raceagg['REcnt'] > 1, 'Two or more', raceagg['xlatlongname'])))
        return raceagg.drop('REcnt', axis=1);

    def biodemo(self):
        sql= "select p_pers_data_effdt.emplid,	\
            p_names.last_name ||','||(case	when p_namesc.first_name <> ' ' then	p_namesc.first_name	else p_names.first_name	end	)|| COALESCE(' ' || p_names.middle_name, '') as university_name,\
        (case	when p_namesc.first_name <> ' ' then	p_namesc.first_name	else p_names.first_name	end	) as university_first_name,	\
        p_names.middle_name                    as university_middle_name,	\
        p_names.last_name                      as university_last_name,	\
        p_names.name_suffix,	\
        p_person.birthdate,	\
        p_pers_data_effdt.sex,	\
        p_scc_caf_persbiog.scc_caf_attr_tval   as gender_pref,	\
        p_person.dt_of_death,	\
        case	when p_person.dt_of_death is null then	'N'	else	'Y'	end as deceased_flag,p_scc_caf_persbioh.scc_caf_attr_tval   as tribal_affiliation,\
        residency_off.city as res_city,	residency_off.country as res_country,	residency_off.county as res_county,	residency_off.state as res_state \
        from siscs.p_pers_data_effdt_av p_pers_data_effdt \
        left outer join (select * from siscs.p_names_av  where edw_actv_ind='Y' and edw_curr_ind='Y')     p_namesc \
        on p_pers_data_effdt.emplid = p_namesc.emplid	and p_namesc.name_type = 'PRF' \
        left outer join (select * from siscs.p_names_av  where edw_actv_ind='Y' and edw_curr_ind='Y')     p_names \
        on p_pers_data_effdt.emplid = p_names.emplid	and p_names.name_type = 'PRI' \
        left outer join (select * from siscs.p_scc_caf_persbio_av  where edw_actv_ind='Y' and edw_curr_ind='Y') p_scc_caf_persbiog \
        on p_pers_data_effdt.emplid = p_scc_caf_persbiog.emplid	and p_scc_caf_persbiog.scc_caf_attrib_nm = 'MSU_CC_GENDER_PREF'	\
            left outer join (select * from siscs.p_scc_caf_persbio_av where edw_actv_ind='Y' and edw_curr_ind='Y')  p_scc_caf_persbioh \
        on p_pers_data_effdt.emplid = p_scc_caf_persbioh.emplid	 and p_scc_caf_persbioh.scc_caf_attrib_nm = 'MSU_CC_TRIB_AFFL_NBR'	\
        left join (select * from siscs.p_person_av   where edw_actv_ind='Y' and edw_curr_ind='Y') p_person \
        on  p_pers_data_effdt.emplid = p_person.emplid	\
        left  join (SELECT	a.emplid,	a.acad_career,	a.city,	a.state,	a.county,	a.country,	\
        ROW_NUMBER() OVER(	PARTITION BY a.emplid	ORDER BY	a.acad_career DESC	) AS res_rank	\
        FROM	siscs.p_residency_off_av a	\
        WHERE	(a.effective_term = (	SELECT	MIN(a_et.effective_term)	FROM	siscs.p_residency_off_v a_et \
        WHERE	a.emplid = a_et.emplid	AND a.acad_career = a_et.acad_career	)or a.effective_term is null	) \
        AND a.edw_actv_ind='Y' and a.edw_curr_ind='Y')    residency_off \
        ON p_pers_data_effdt.emplid = residency_off.emplid and residency_off.res_rank = 1 \
        where p_pers_data_effdt.edw_actv_ind='Y' and p_pers_data_effdt.edw_curr_ind='Y' \
        and p_pers_data_effdt.emplid in (select emplid from siscs.p_stdnt_car_term_av where edw_curr_ind='Y' and edw_actv_ind='Y' and strm="+ self.term +" ) \
        and (p_pers_data_effdt.effdt = (	select	max(a_ed.effdt)	from	siscs.p_pers_data_effdt_av a_ed	\
        where	p_pers_data_effdt.emplid = a_ed.emplid	\
        and a_ed.effdt <= sysdate	and a_ed.edw_actv_ind='Y' and a_ed.edw_curr_ind='Y'	\
        ) or p_pers_data_effdt.effdt is null)	\
        and ( p_scc_caf_persbiog.effdt = (	\
        select	max(g_ed.effdt)	from	siscs.p_scc_caf_persbio_av g_ed	\
        where	p_scc_caf_persbiog.emplid = g_ed.emplid	\
        and g_ed.edw_actv_ind='Y' and g_ed.edw_curr_ind='Y' and g_ed.effdt <= sysdate	) \
        or p_scc_caf_persbiog.effdt is null ) and ( p_scc_caf_persbioh.effdt = ( select	max(h_ed.effdt)	from siscs.p_scc_caf_persbio_av h_ed \
        where	p_scc_caf_persbioh.emplid = h_ed.emplid	and h_ed.edw_actv_ind='Y' and h_ed.edw_curr_ind='Y'	and h_ed.effdt <= sysdate	)\
        or p_scc_caf_persbioh.effdt is null ) \
        and (p_names.effdt = (	select	max(b_ed.effdt)	from	siscs.p_names_av b_ed	\
        where	p_names.emplid = b_ed.emplid	and p_names.name_type = b_ed.name_type	and b_ed.edw_actv_ind='Y' and b_ed.edw_curr_ind='Y' and b_ed.effdt <= sysdate	) \
        OR p_names.effdt IS NULL)	\
        and ( p_namesc.effdt = (	select	max(c_ed.effdt)	from	siscs.p_names_av c_ed	where	p_namesc.emplid = c_ed.emplid	and p_namesc.name_type = c_ed.name_type	\
        and c_ed.edw_actv_ind='Y' and c_ed.edw_curr_ind='Y'	and c_ed.effdt <= sysdate	)	or p_namesc.effdt is null )"
        df = pd.read_sql(sql, ct.EDW)
        return df

    def enrlschbase(self, date):
        sql= " select b.emplid, b.acad_career, res.residency ,res.admission_res  ,res.tuition_res ,res.FIN_AID_FED_RES, sum(a.unt_taken) as sum_unt_taken, \
        sum(unt_billing) as sum_unt_billing from siscs.p_stdnt_enrl_av a inner join siscs.P_STDNT_CAR_TERM_av b on a.emplid=b.emplid \
         and a.strm=b.strm and a.acad_career=b.acad_career  inner join siscs.p_residency_off_av res   on a.emplid=res.emplid \
          and a.acad_career=res.acad_career and a.institution=res.institution and  '"+date+ "' between res.edw_eff_start_date and res.edw_eff_end_date \
            and res.effective_term= ( select max(effective_term) from siscs.p_residency_off_av r where b.emplid= r.emplid   and  b.acad_career= r.acad_career \
        and b.institution= r.institution  and '"+date+"' between r.edw_eff_start_date and r.edw_eff_end_date and b.strm >= r.effective_term) \
        where a.strm="+self.term +" and a.stdnt_enrl_status='E'  and (  b.ELIG_TO_ENROLL='Y'     or withdraw_code <> 'WDR') and '"+date+"' between a.edw_eff_start_date \
        and a.edw_eff_end_date  and '"+date+"' between b.edw_eff_start_date and b.edw_eff_end_date group by b.emplid, b.acad_career,res.residency  ,\
        res.admission_res  ,res.tuition_res  ,res.FIN_AID_FED_RES"
        df= pd.read_sql(sql,ct.EDW)
        return df

    def enrlschagg(self,date):
        basedf = self.enrlschbase(date)
        df1= basedf.groupby(['acad_career','tuition_res'])['sum_unt_taken','sum_unt_billing'].sum()
        df = basedf.groupby(['acad_career','tuition_res']).size().reset_index().join(df1, on=['acad_career','tuition_res'])
        df.columns=['acad_career', 'tuition_res', 'N', 'sum_unt_taken', 'sum_unt_billing']
        df['Date']=date
        return df



