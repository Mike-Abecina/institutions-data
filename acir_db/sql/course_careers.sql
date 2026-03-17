select 
            cc.id course_career_id,
            cc.course_id,
            c.id career_id,
            c.name,
            c.anzsco,
            c.description
            /*c.education_training,
            c.additional_information,
            c.employment_opportunities*/
        from course_career cc
                inner join careers c on cc.career_id = c.id
                inner  join courses c2 on cc.course_id = c2.id
        where c.deleted_at is null
        and c2.deleted_at is null
        
