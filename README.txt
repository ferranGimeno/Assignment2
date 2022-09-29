Tables:
Data from table User, tabulated:
id    has_labels
----  ------------

Data from table Activity, tabulated:
id    user_id    transportation_mode    start_date_time    end_date_time
----  ---------  ---------------------  -----------------  ---------------

Data from table TrackPoint, tabulated:
id    activity_id    lat    lon    altitude    date_days    date_time
----  -------------  -----  -----  ----------  -----------  -----------

1. How many users, activities and trackpoints are there in the dataset (after it is
inserted into the database).
SELECT COUNT(*) FROM User
SELECT COUNT(*) FROM Activity
SELECT COUNT(*) FROM TrackPoint

2. Find the average number of activities per user.
SELECT AVG(*) FROM User as u, Activity as a WHERE u.id = a.userid

3. Find the top 20 users with the highest number of activities.
SELECT u.id FROM x WHERE x LIMIT 20 ASC

4. Find all users who have taken a taxi.
SELECT u.id FROM User as u, Activity as a WHERE u.id = a.user_id AND a.transportation_mode LIKE "taxi"

5. Find all types of transportation modes and count how many activities that are tagged with these transportation mode labels. Do not count the rows where the mode is null.
SELECT transportation_mode, count(*) from Activity GROUP BY transportation_mode

6.
    a) Find the year with the most activities.

        WITH Q as (SELECT YEAR(start_date_time) FROM Activity GROUP BY YEAR(start_date_time) ORDER BY YEAR(start_date_time));
        SELECT TOP 1 * FROM Q;

    b) Is this also the year with most recorded hours?

7. Find the total distance (in km) walked in 2008, by user with id=112.

8. Find the top 20 users who have gained the most altitude meters.
    ○ Output should be a table with (id, total meters gained per user).
    ○ Remember that some altitude-values are invalid
    ○ Tip: ∑ (𝑡𝑝 𝑛. 𝑎𝑙𝑡𝑖𝑡𝑢𝑑𝑒 − 𝑡𝑝 𝑛−1. 𝑎𝑙𝑡𝑖𝑡𝑢𝑑𝑒), 𝑡𝑝 𝑛. 𝑎𝑙𝑡𝑖𝑡𝑢𝑑𝑒 > 𝑡𝑝 𝑛−1. 𝑎𝑙𝑡𝑖𝑡𝑢𝑑𝑒

9. Find all users who have invalid activities, and the number of invalid activities per user
    ○ An invalid activity is defined as an activity with consecutive trackpoints where the timestamps deviate with at least 5 minutes.

10.Find the users who have tracked an activity in the Forbidden City of Beijing.
    ○ In this question you can consider the Forbidden City to have coordinates that correspond to: lat 39.916, lon 116.397.
    WITH Q as (SELECT * FROM Activity JOIN Trackpoint ON Activity.id = Trackpoint.activity_id)
    SELECT user_id FROM Q WHERE lat = 39.916 and lon = 116.397

11.Find all users who have registered transportation_mode and their most used transportation_mode.
    ○ The answer should be on format (user_id, most_used_transportation_mode) sorted on user_id.
    ○ Some users may have the same number of activities tagged with e.g. walk and car. In this case it is up to you to decide which transportation mode to include in your answer (choose one).
    ○ Do not count the rows where the mode is null.