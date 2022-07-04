create database milestone;


use milestone;

-- Badges
DROP TABLE IF EXISTS Badges CASCADE;
CREATE TABLE Badges (
   Id                int         PRIMARY KEY ,
   UserId            int         not NULL    ,
   Name              text        not NULL    ,
   Date              timestamp   not NULL
);


-- Comments
DROP TABLE IF EXISTS Comments CASCADE;
CREATE TABLE Comments (
    Id                     int PRIMARY KEY    ,
    PostId                 int not NULL       , 
    Score                  int not NULL       ,
    Text                   text               ,
    CreationDate           timestamp not NULL , 
    UserId                 int                
);

-- PostHistory
DROP TABLE IF EXISTS PostHistory CASCADE;
CREATE TABLE PostHistory (
    Id                 int  PRIMARY KEY   ,
    PostHistoryTypeId  int                ,
    PostId             int                ,
    RevisionGUID       text               ,
    CreationDate       timestamp not NULL ,
    UserId             int                ,
    PostText           text              
);

-- PostLinks
DROP TABLE IF EXISTS PostLinks CASCADE;
CREATE TABLE PostLinks (
   Id                int         PRIMARY KEY ,
   CreationDate      timestamp   not NUll    ,
   PostId            int         not NULL    ,
   RelatedPostId     int         not NULL    ,
   LinkTypeId        int         not Null
);

-- Posts
DROP TABLE IF EXISTS Posts CASCADE;
CREATE TABLE Posts (
    Id                     int PRIMARY KEY    ,
    PostTypeId             int not NULL       ,
    AcceptedAnswerId       int                ,
    ParentId               int                ,
    CreationDate           timestamp not NULL ,
    Score                  int                ,
    ViewCount              int                ,
    Body                   text               ,
    OwnerUserId            int                ,
    LastEditorUserId       int                ,
    LastEditorDisplayName  text               ,
    LastEditDate           timestamp          ,
    LastActivityDate       timestamp          ,
    Title                  text               ,
    Tags                   text               ,
    AnswerCount            int                ,
    CommentCount           int                ,
    FavoriteCount          int                ,
    ClosedDate             timestamp          ,
    CommunityOwnedDate     timestamp          
);

-- Tags
DROP TABLE IF EXISTS Tags CASCADE;
CREATE TABLE Tags (
    Id                    int  PRIMARY KEY ,
    TagName               text not NULL    ,
    Count                 int              ,
    ExcerptPostId         int              ,
    WikiPostId            int              
);

-- Users
DROP TABLE IF EXISTS Users CASCADE;
CREATE TABLE Users (
   Id                int         PRIMARY KEY ,
   Reputation        int         not NULL    ,
   CreationDate      timestamp   not NULL    ,
   DisplayName       varchar(40) not NULL    ,
   LastAccessDate    timestamp               ,
   WebsiteUrl        TEXT                    ,
   Location          TEXT                    ,
   AboutMe           TEXT                    ,
   Views             int         not NULL    ,
   UpVotes           int         not NULL    ,
   DownVotes         int         not NULL    ,
   ProfileImageUrl   text                    ,
   Age               int                     ,
   AccountId         int                     
);

-- Votes
DROP TABLE IF EXISTS Votes CASCADE;
CREATE TABLE Votes (
   Id                int         PRIMARY KEY ,
   PostId            int         , -- not NULL    ,
   VoteTypeId        int         not NULL    ,
   UserId            int                     ,
   CreationDate      timestamp   not NULL    ,
   BountyAmount      int                     
);
    
   select * from milestone.Comments; 


-- Foriegn Key
-- Badges with Users
ALTER TABLE badges ADD CONSTRAINT fk_badges_userid FOREIGN KEY (userid) REFERENCES users (id);

-- Comments with Users and Posts
ALTER TABLE Comments ADD CONSTRAINT fk_comments_userid FOREIGN KEY (userid) REFERENCES users (id);
ALTER TABLE Comments ADD CONSTRAINT fk_comments_postid FOREIGN KEY (postid) REFERENCES posts (id);

-- Posthistory with Users and Posts
ALTER TABLE Posthistory ADD CONSTRAINT fk_posthistory_userid FOREIGN KEY (userid) REFERENCES users (id);
ALTER TABLE Posthistory ADD CONSTRAINT fk_posthistory_postid FOREIGN KEY (postid) REFERENCES posts (id);

-- Postlinks with Posts
ALTER TABLE Postlinks ADD CONSTRAINT fk_postlinks_postid FOREIGN KEY (postid) REFERENCES posts (id);
ALTER TABLE Postlinks ADD CONSTRAINT fk_postlinks_relatedpostid FOREIGN KEY (relatedpostid) REFERENCES posts (id);

-- Posts with Posts, Users
ALTER TABLE Posts ADD CONSTRAINT fk_posts_parentid FOREIGN KEY (parentid) REFERENCES posts (id);
ALTER TABLE Posts ADD CONSTRAINT fk_posts_owneruserid FOREIGN KEY (owneruserid) REFERENCES users (id);
ALTER TABLE Posts ADD CONSTRAINT fk_posts_lasteditoruserid FOREIGN KEY (lasteditoruserid) REFERENCES users (id);

-- Votes with Users and Posts
ALTER TABLE Votes ADD CONSTRAINT fk_votes_userid FOREIGN KEY (userid) REFERENCES users (id);
ALTER TABLE Votes ADD CONSTRAINT fk_votes_postid FOREIGN KEY (postid) REFERENCES posts (id);


-- LoadData

-- Users
LOAD XML LOCAL INFILE "/Users/syedaman/Downloads/3dprinting.stackexchange.com/Users.xml" INTO TABLE milestone.Users;
SELECT count(*) FROM milestone.Users;

-- Badges
LOAD XML LOCAL INFILE "/Users/syedaman/Downloads/3dprinting.stackexchange.com/Badges.xml" INTO TABLE Badges;
SELECT count(*) FROM Badges;

-- Posts
LOAD XML LOCAL INFILE "/Users/syedaman/Downloads/3dprinting.stackexchange.com/Posts.xml"  INTO TABLE Posts;
SELECT count(*) FROM Posts;

-- Comments
LOAD XML LOCAL INFILE "/Users/syedaman/Downloads/3dprinting.stackexchange.com/Comments.xml"  INTO TABLE Comments;
SELECT count(*) FROM Comments;

-- PostHistory
LOAD XML LOCAL INFILE "/Users/syedaman/Downloads/3dprinting.stackexchange.com/PostHistory.xml"  INTO TABLE PostHistory;
SELECT count(*) FROM PostHistory;

-- Postlinks
LOAD XML LOCAL INFILE "/Users/syedaman/Downloads/3dprinting.stackexchange.com/PostLinks.xml" INTO TABLE PostLinks;
SELECT count(*) FROM PostLinks;

-- Tags
LOAD XML LOCAL INFILE "/Users/syedaman/Downloads/3dprinting.stackexchange.com/Tags.xml"  INTO TABLE Tags;
SELECT count(*) FROM Tags;

-- Votes

LOAD XML LOCAL INFILE "/Users/syedaman/Downloads/3dprinting.stackexchange.com/Votes.xml"  INTO TABLE Votes;
SELECT count(*) FROM Votes;


-- Index Creation
-- Badges                                            
CREATE INDEX badges_user_id_idx on Badges(UserId) USING btree;
CREATE INDEX badges_name_idx on Badges(Name(255)) USING btree;
CREATE INDEX badges_date_idx on Badges(Date) USING btree;

-- Comments         
CREATE INDEX cmnts_score_idx ON Comments(Score)  USING btree;
CREATE INDEX cmnts_postid_idx ON Comments(PostId) USING btree;
CREATE INDEX cmnts_creation_date_idx ON Comments (CreationDate) USING btree;
CREATE INDEX cmnts_userid_idx ON Comments(UserId) USING btree;
       
-- PostHistory         
CREATE INDEX ph_post_type_id_idx ON PostHistory(PostHistoryTypeId) USING btree;
CREATE INDEX ph_postid_idx ON PostHistory(PostId) USING btree;
CREATE INDEX ph_revguid_idx ON PostHistory(RevisionGUID(555)) USING btree;
CREATE INDEX ph_creation_date_idx ON PostHistory(CreationDate) USING btree;
CREATE INDEX ph_userid_idx ON PostHistory(UserId) USING btree;

-- PostLinks        
CREATE INDEX postlinks_post_id_idx on PostLinks(PostId) USING btree;
CREATE INDEX postlinks_related_post_id_idx on PostLinks(RelatedPostId) USING btree;
       
-- Posts      
CREATE INDEX posts_post_type_id_idx ON Posts(PostTypeId) USING btree;
CREATE INDEX posts_score_idx ON Posts(Score) USING btree;
CREATE INDEX posts_creation_date_idx ON Posts(CreationDate) USING btree;
CREATE INDEX posts_owner_user_id_idx ON Posts(OwnerUserId) USING btree;
CREATE INDEX posts_answer_count_idx ON Posts(AnswerCount) USING btree;
CREATE INDEX posts_comment_count_idx ON Posts(CommentCount) USING btree;
CREATE INDEX posts_favorite_count_idx ON Posts(FavoriteCount) USING btree;
CREATE INDEX posts_viewcount_idx ON Posts(ViewCount) USING btree;
CREATE INDEX posts_accepted_answer_id_idx ON Posts(AcceptedAnswerId) USING btree;
CREATE INDEX posts_parent_id_idx ON Posts(ParentId) USING btree;

-- Tags     
CREATE INDEX tags_count_idx on Tags(Count) USING btree;
CREATE INDEX tags_name_idx on Tags(TagName(255)) USING btree;
       
-- Users  
CREATE INDEX user_acc_id_idx ON Users(AccountId) USING btree;
CREATE INDEX user_display_idx ON Users(DisplayName) USING btree;
CREATE INDEX user_up_votes_idx ON Users(UpVotes) USING btree;
CREATE INDEX user_down_votes_idx ON Users(DownVotes) USING btree;
CREATE INDEX user_created_at_idx ON Users(CreationDate) USING btree;
       
-- Votes
CREATE INDEX votes_post_id_idx on Votes(PostId) USING btree;
CREATE INDEX votes_type_idx on Votes(VoteTypeId) USING btree;
CREATE INDEX votes_creation_date_idx on Votes(CreationDate) USING btree;





-- Problem 1
select Id, DisplayName, Reputation from Users order by Reputation desc LIMIT 10;


-- Problem 2


select u.DisplayName,p.title from Users u , Posts p where u.DisplayName = 'CJK' and u.id=p.OwnerUserId  and p.PostTypeId=1;

-- Problem 3
select u.DisplayName,p.title from Users u , Posts p where u.DisplayName like '%nau%' and u.id=p.OwnerUserId  and p.PostTypeId=1;

-- Problem 4


select count(*) , b.Name from Badges b join Users u on u.Id = b.UserId group by b.Name order by count(*) desc limit 10;

-- Problem 5

select u.Id,u.DisplayName,u.reputation,questions_asked.total_no_of_questions_asked
from Users u join
(select u.Id,count(p.PostTypeId) as total_no_of_questions_asked
    from Users u join Posts p on u.Id=p.OwnerUserId
    where u.Reputation > 1000 and p.PostTypeId=1
    group by u.Id) as questions_asked
on u.Id = questions_asked.Id;










