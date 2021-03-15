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
         a.acad_level_bot, a.TOT_PROJ_UNITS, x.XLATSHORTNAME, a.acad_plan, c.acad_plan_descr,\
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



