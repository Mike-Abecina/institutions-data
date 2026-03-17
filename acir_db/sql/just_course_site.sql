SELECT DISTINCT
    c.id  AS course_id,
    cs.site_id, 
    --cs.id AS course_site_id
FROM course_sites cs
INNER JOIN courses c ON cs.course_id = c.id;
