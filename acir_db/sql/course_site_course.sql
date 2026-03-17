select distinct c.course_id,c.course_name, c.cs_deleted_by, exists(select 1 from course_career where course_id = c.course_id) has_course_careers   from (
select cs.id                              course_site_id,
                o.id                              organisation_id,
                o.name                            organisation_name,
                o.slug                            organisation_slug,  
                o.organisation_type_id,
                o.cricos_code                     organisation_cricos_code,
                o.rto_code                        organisation_rto_code,
                o.web_address                     organisation_website_url,
                ot.name                           organisation_type_name,
                st.abbreviation                   organisation_state,
                EXISTS(SELECT 1 FROM app_organisation_client_profile aocp INNER JOIN client_profiles cp ON aocp.client_profile_id = cp.id
                    WHERE aocp.app_id = 1 AND cp.active = 1 AND aocp.organisation_id = o.id LIMIT 1) AS is_active_gug_client,
                EXISTS(SELECT 1 FROM app_organisation_client_profile aocp INNER JOIN client_profiles cp ON aocp.client_profile_id = cp.id
                    WHERE aocp.app_id = 2 AND cp.active = 1 AND aocp.organisation_id = o.id LIMIT 1) AS is_active_gsg_client,
                EXISTS(SELECT 1 FROM app_organisation_client_profile aocp INNER JOIN client_profiles cp ON aocp.client_profile_id = cp.id
                    WHERE aocp.app_id = 3 AND cp.active = 1 AND aocp.organisation_id = o.id LIMIT 1) AS is_active_sia_client,
                c.id                              course_id,
                c.name                            course_name,
                c.title                           course_title,
                c.slug                            course_slug,
                c.description                     course_description,
                c.abbreviation                    course_abbreviation,
                c.designed_for                    course_designed_for,
                c.structure                       course_structure,
                c.subjects                        course_subjects,
                c.entry_requirements              course_entry_requirements,
                c.standard_entry_requirements     course_standard_entry_requirements,
                c.alternate_entry_requirements    course_alternate_entry_requirements,
                c.recognition                     course_recognition,
                c.study_pathways                  course_study_pathways,
                c.other                           course_other_info,
                c.accred_code                     course_accred_code,
                c.overseas_student_entry          overseas_student_entry,
                c.vet_course_is_current           vet_course_is_current,
                c.keyword_search                  keyword_search,
                c.gg_title                        course_gg_title,
                c.qualification_id,
                cl.id                             course_level_id,
                cl.name                           course_level_name,
                f.id                              faculty_id,
                f.name                            faculty_name,
                cs.year                           course_site_year,
                cs.cricos                         course_site_cricos,
                cs.overall_toefl_score,
                cs.overall_ielts_score,
                cs.course_start_date              course_start_date,
                cs.application_enrolment          application_enrolment,  
                cs.domestic_full_fee,
                cs.domestic_full_fee_available,
                cs.domestic_full_fee_year,
                cs.overseas_full_fee,
                cs.overseas_full_fee_available,
                cs.overseas_full_fee_year,
                cs.fee_comments,
                cs.other_significant_fees,
                cs.csp_fee,
                cs.csp_fee_year,
                cs.vic_gov_t_subs_fee_available,
                cs.vic_gov_t_subs_fee,
                cs.vic_gov_t_subs_fee_year,
                cs.free_tafe_course,
                cs.course_web_address,
                s.id                              site_id,
                s.name                            site_name,
                site_st.abbreviation              site_state,
                site_st.name                      site_state_name_full,
                s.study_area                      site_study_area,
                s.accommodation                   site_accommodation,
                s.transport                       site_transport,
                s.comments                        site_comments,
                s.primary_site                    site_primary_site,
                s.street1                         site_street1,
                s.street2                         site_street2,
                s.suburb                          site_suburb,
                s.postcode                        site_postcode,
                s.latitude                        site_latitude,
                s.longitude                       site_longitude,
                s.in_vic_scope                    site_in_vic_scope,
                s.in_qld_scope                    site_in_qld_scope,
                r.id                              site_region_id,
                r.name                            site_region_name,
                r.name_short                      site_region_name_short,
                cts.id                            country_id,
                cts.name                          country_name,
                case
                    when c.created_at is not null and c.created_at != '0000-00-00 00:00:00' then c.created_at
                    when c.created_at is not null and c.created_at = '0000-00-00 00:00:00' then '1900-01-01 00:00:00'
                    end                           created_at,
                cs.deleted_by                     cs_deleted_by,
                case
                    when cs.deleted_at is not null then 'course_site_deleted'
                    when cs.archived_at is not null then 'course_site_archived'
                    when c.deleted_at is not null then 'course_deleted'
                    when s.deleted_at is not null then 'site_deleted'
                    when o.deleted_at is not null then 'organisation_deleted'
                    end                           deleted_at_info,
                case
                    when cs.deleted_at is not null and cs.deleted_at != '0000-00-00 00:00:00' then cs.deleted_at
                    when cs.deleted_at is not null and cs.deleted_at = '0000-00-00 00:00:00' then '1900-01-01 00:00:00'
                    when cs.archived_at is not null then cs.archived_at
                    when c.deleted_at is not null then c.deleted_at
                    when s.deleted_at is not null then s.deleted_at
                    when o.deleted_at is not null then o.deleted_at
                    end                           deleted_at
            from course_sites cs
                    inner join sites s on cs.site_id = s.id
                    inner join courses c on cs.course_id = c.id
                    inner join organisations o on s.organisation_id = o.id
                    left join states st on o.state_id = st.id
                    left join states site_st on s.state_id = site_st.id
                    left join organisation_types ot on o.organisation_type_id = ot.id
                    left join faculties f on c.faculty_id = f.id
                    left join regions r on r.id = s.region_id
                    left join countries cts on cts.id = s.country_id
                    left join course_levels cl on cl.id = c.course_level_id
            ) c