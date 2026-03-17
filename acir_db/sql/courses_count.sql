-- Count of active course-site-organisation rows (deleted_at_info IS NULL).
-- Used by aggregate_courses.py to determine how many batches are needed.
-- Mirrors the base subquery in courses_batch.sql exactly.
SELECT COUNT(*) AS total_courses
FROM (
    select cs.id                              course_site_id,
                o.id                              organisation_id,
                c.id                              course_id,
                case
                    when cs.deleted_at is not null then 'course_site_deleted'
                    when cs.archived_at is not null then 'course_site_archived'
                    when c.deleted_at is not null then 'course_deleted'
                    when s.deleted_at is not null then 'site_deleted'
                    when o.deleted_at is not null then 'organisation_deleted'
                    end                           deleted_at_info
            from course_sites cs
                    inner join sites s on cs.site_id = s.id
                    inner join courses c on cs.course_id = c.id
                    inner join organisations o on s.organisation_id = o.id
) c
WHERE c.deleted_at_info IS NULL
