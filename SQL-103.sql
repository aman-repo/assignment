use assignment;

-- Problem-1. #self

select Id, DisplayName, Reputation from Users 
where Reputation > (select Reputation from users where DisplayName = 'joeqwerty');



-- Problem-2 #EXT

select u.Id, u.DisplayName, u.Reputation from Users u left outer join badges b on u.Id=b.UserId 
join Comments c on u.Id=c.UserId 
where u.Reputation > 1000 and c.Score > 10 and (b.Name='Nice Answer' or b.name='Good Answer' or b.name='Great Answer')
order by u.reputation;


-- Problem-3. #DUPE 

select c1.Text from comments c1
where c1.postId In (SELECT distinct (c2.PostId)
FROM Comments c2
         INNER JOIN
     Comments c3
       ON
         c2.PostId = c3.PostId where TIMESTAMPDIFF(MINUTE, c2.CreationDate, c3.CreationDate) Between 0 and 1);
         
         
-- Problem-4. #bulkupdate

select distinct owneruserid,
      CASE When PostTypeId=1 then count(*)
       else null end AS TOTAL_questions_asked,
       CASE when PostTypeId=2 then count(*)
        else null end AS TOTAL_questions_answered
from posts
group by owneruserid,PostTypeid;



-- Problem-6. #LateLateef

Update badges inner join posts on posts.Owneruserid = badges.UserId
   set badges.Name = (CASE
        WHEN timestampdiff(DAY, truncate(posts.CreationDate,'DAY'), truncate(posts.LastEditDate,'DAY')) >= 365 THEN 'LATELATEEF'
        ELSE (badges.Name) END);

         









         





