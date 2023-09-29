----------------------------------------- PART 3.1 -----------------------------------------
CREATE OR replace FUNCTION TransferredPoints_humanity() RETURNS TABLE (
    Peer1 varchar(50), Peer2 varchar(50), PointsAmount int
  ) AS $$
BEGIN
  RETURN query (
    SELECT t1.checking_peer_nickname Peer1, t1.peer_being_checked_nickname Peer2, t1.num_transferred_points - t2.num_transferred_points PointsAmount
FROM TransferredPoints t1
JOIN TransferredPoints t2 ON
  t1.checking_peer_nickname = t2.peer_being_checked_nickname
  AND t1.peer_being_checked_nickname = t2.checking_peer_nickname
WHERE(
        t1.num_transferred_points - t2.num_transferred_points
      ) <= 0
GROUP BY t1.peer_being_checked_nickname, t1.checking_peer_nickname, t1.num_transferred_points, t2.num_transferred_points
  )
UNION
(
  SELECT checking_peer_nickname Peer1, peer_being_checked_nickname Peer2, num_transferred_points
FROM TransferredPoints
EXCEPT
  SELECT t1.checking_peer_nickname Peer1, t1.peer_being_checked_nickname Peer2, t1.num_transferred_points PointsAmount
FROM TransferredPoints t1
JOIN TransferredPoints t2 ON
t1.checking_peer_nickname = t2.peer_being_checked_nickname
AND t1.peer_being_checked_nickname = t2.checking_peer_nickname
GROUP BY t1.peer_being_checked_nickname, t1.checking_peer_nickname, t1.num_transferred_points, t2.num_transferred_points
);
END;

$$ LANGUAGE plpgsql;
----------------------------------------- TEST 3.1 -----------------------------------------
--SELECT * FROM TransferredPoints_humanity();
----------------------------------------- PART 3.2 -----------------------------------------
CREATE OR REPLACE FUNCTION get_user_xp_for_task() RETURNS TABLE (
    peer VARCHAR(50), task VARCHAR(50), xp INTEGER
  ) AS $$ BEGIN RETURN query (
    SELECT checks.peer_nickname, checks.task_name, xp.num_xp_received
FROM checks
JOIN xp ON
checks.id = xp.check_id
  );
END;

$$ LANGUAGE plpgsql;
----------------------------------------- TEST 3.2 -----------------------------------------
--SELECT * FROM get_user_xp_for_task();
----------------------------------------- PART 3.3 -----------------------------------------
CREATE OR REPLACE FUNCTION peers_in_campus(date_param DATE) RETURNS SETOF VARCHAR(50) AS $$ BEGIN RETURN QUERY
SELECT peer_nickname
FROM TimeTracking
WHERE date = date_param
AND state = 1
EXCEPT
SELECT peer_nickname
FROM TimeTracking
WHERE date = date_param
AND state = 2;
END;

$$ LANGUAGE plpgsql;
----------------------------------------- TEST 3.3 -----------------------------------------
--insert into TimeTracking values 
--((select max(id) from TimeTracking) + 1, 'iseadra', '01.02.23', '11:59', 1),
--((select max(id) from TimeTracking) + 2, 'qreiko', '01.02.23', '10:59', 1),
--((select max(id) from TimeTracking) + 3, 'iseadra', '01.02.23', '21:59', 2);
--SELECT * FROM peers_in_campus('2023-01-02');
----------------------------------------- PART 3.4 -----------------------------------------
CREATE OR REPLACE PROCEDURE change_in_quantity(IN REF refcursor) AS $$ BEGIN OPEN REF FOR (
    (
      SELECT tab1.peer, tab2.PointsChange
FROM(
          (
            SELECT checking_peer_nickname peer
FROM transferredpoints
GROUP BY transferredpoints.checking_peer_nickname
          )
EXCEPT (
              SELECT checking_peer_nickname Peer
FROM(
                  SELECT checking_peer_nickname, sum(num_transferred_points) PointsChange
FROM transferredpoints
GROUP BY transferredpoints.checking_peer_nickname
                ) t1
JOIN (
                  SELECT peer_being_checked_nickname, sum(num_transferred_points) * -1
                  minus
FROM transferredpoints
GROUP BY transferredpoints.peer_being_checked_nickname
                ) t2 ON
t1.checking_peer_nickname = t2.peer_being_checked_nickname
            )
        ) AS tab1
JOIN (
          SELECT checking_peer_nickname, sum(num_transferred_points) PointsChange
FROM transferredpoints
GROUP BY transferredpoints.checking_peer_nickname
        ) AS tab2 ON
tab2.checking_peer_nickname = tab1.peer
    )
UNION
    (
      SELECT tab1.peer, tab2.minus
FROM(
          (
            SELECT peer_being_checked_nickname peer
FROM transferredpoints
GROUP BY transferredpoints.peer_being_checked_nickname
          )
EXCEPT (
              SELECT checking_peer_nickname Peer
FROM(
                  SELECT checking_peer_nickname, sum(num_transferred_points) PointsChange
FROM transferredpoints
GROUP BY transferredpoints.checking_peer_nickname
                ) t1
JOIN (
                  SELECT peer_being_checked_nickname, sum(num_transferred_points) * -1
                  minus
FROM transferredpoints
GROUP BY transferredpoints.peer_being_checked_nickname
                ) t2 ON
t1.checking_peer_nickname = t2.peer_being_checked_nickname
            )
        ) AS tab1
JOIN (
          SELECT peer_being_checked_nickname, sum(num_transferred_points) * -1
          minus
FROM transferredpoints
GROUP BY transferredpoints.peer_being_checked_nickname
        ) AS tab2 ON
tab2.peer_being_checked_nickname = tab1.peer
    )
  )
UNION
(
  SELECT checking_peer_nickname Peer,(t1.PointsChange + t2.minus) PointsChange
FROM(
      SELECT checking_peer_nickname, sum(num_transferred_points) PointsChange
FROM transferredpoints
GROUP BY transferredpoints.checking_peer_nickname
    ) t1
JOIN (
      SELECT peer_being_checked_nickname, sum(num_transferred_points) * -1
      minus
FROM transferredpoints
GROUP BY transferredpoints.peer_being_checked_nickname
    ) t2 ON
t1.checking_peer_nickname = t2.peer_being_checked_nickname
)
ORDER BY PointsChange DESC;
END;

$$ LANGUAGE plpgsql;
----------------------------------------- TEST 3.4 -----------------------------------------
--BEGIN;
--CALL change_in_quantity('ref');
--FETCH ALL IN "ref";
--END;
----------------------------------------- PART 3.5 -----------------------------------------
CREATE OR REPLACE PROCEDURE change_in_quantitt_part5(IN REF refcursor) AS $$ BEGIN OPEN REF FOR WITH table_left AS (
    SELECT peer1, sum(pointsamount) pointsamount
FROM TransferredPoints_humanity()
GROUP BY transferredpoints_humanity.peer1
  ), table_right AS (
    SELECT t1.peer2, sum(t1.pointsamount) pointsamount
FROM(
        SELECT peer1, peer2,(pointsamount * -1) pointsamount
FROM TransferredPoints_humanity()
      ) t1
GROUP BY t1.peer2
  ), table_sum AS (
    SELECT peer1,(t1.pointsamount + t2.pointsamount) PointsChange
FROM table_left t1
JOIN table_right t2 ON
t1.peer1 = t2.peer2
  ), without_amount_left AS (
    SELECT peer1
FROM table_left
EXCEPT
    SELECT peer1
FROM table_sum
  ), without_amount_right AS (
    SELECT peer2
FROM table_right
EXCEPT
    SELECT peer1
FROM table_sum
  ), true_table_left AS (
    SELECT without_amount_left.peer1 Peer, table_left.pointsamount PointsChange
FROM without_amount_left
JOIN table_left ON
without_amount_left.peer1 = table_left.peer1
  ), true_table_right AS (
    SELECT without_amount_right.peer2 Peer, table_right.pointsamount PointsChange
FROM without_amount_right
JOIN table_right ON
without_amount_right.peer2 = table_right.peer2
  ) (
    SELECT *
FROM true_table_left
  )
UNION
(
  SELECT *
FROM true_table_right
)
UNION
(
  SELECT *
FROM table_sum
)
ORDER BY PointsChange DESC;
END;

$$ LANGUAGE plpgsql;
----------------------------------------- TEST 3.5 -----------------------------------------
--BEGIN;
--CALL change_in_quantitt_part5('ref');
--FETCH ALL IN "ref";
--END;
----------------------------------------- PART 3.6 -----------------------------------------
CREATE OR REPLACE PROCEDURE most_frequently_checked_task_for_each_day(IN REF refcursor) AS $$ BEGIN OPEN REF FOR
SELECT check_date AS DAY, substring(
    task_name
    FROM '^[^_]+'
  ) AS Task
FROM(
    SELECT check_date, task_name, COUNT(*) AS cnt, RANK() OVER (
        PARTITION BY check_date
ORDER BY COUNT(*) DESC
      ) AS RANK
FROM Checks
GROUP BY check_date, task_name
  ) AS ranked
WHERE RANK = 1
ORDER BY check_date DESC;
END;

$$ LANGUAGE plpgsql;
----------------------------------------- TEST 3.6 -----------------------------------------
--BEGIN;
--CALL most_frequently_checked_task_for_each_day('ref');
--FETCH ALL IN "ref";
--END;
----------------------------------------- PART 3.7 -----------------------------------------
CREATE OR replace FUNCTION get_peers_who_made_block_tasks(block varchar) RETURNS TABLE (peer varchar, DAY date) AS $$ BEGIN RETURN query
SELECT peer_nickname AS Peer, check_date AS DAY
FROM checks
JOIN p2p ON
p2p.check_id = checks.id
JOIN verter ON
verter.check_id = checks.id
WHERE task_name = (
    SELECT name
FROM tasks
WHERE name LIKE block || (
        SELECT count(name)
FROM tasks
WHERE name LIKE block || '_\_' || '%'
      ) || '%'
  )
AND p2p_check_status = 'Success'
AND verter_check_status = 'Success';
END;

$$ LANGUAGE plpgsql;
----------------------------------------- TEST 3.7 -----------------------------------------
--select * from get_peers_who_made_block_tasks('C');
----------------------------------------- PART 3.8 -----------------------------------------
CREATE OR REPLACE PROCEDURE recommended_by_friends(IN REF refcursor) AS $$ BEGIN OPEN REF FOR WITH search_friends AS (
    SELECT nickname,(
        CASE
          WHEN nickname = friends.first_peer_nickname THEN second_peer_nickname
ELSE first_peer_nickname
END
      ) AS frineds
FROM peers
JOIN friends ON
peers.nickname = friends.first_peer_nickname
OR peers.nickname = friends.second_peer_nickname
  ), search_reccommend AS (
    SELECT nickname, COUNT(recommended_peer_nickname) AS count_rec, recommended_peer_nickname
FROM search_friends
JOIN recommendations ON
search_friends.frineds = recommendations.peer_nickname
WHERE search_friends.nickname != recommendations.recommended_peer_nickname
GROUP BY nickname, recommended_peer_nickname
  ), search_max AS (
    SELECT nickname, MAX(count_rec) AS max_count
FROM search_reccommend
GROUP BY nickname
  )
SELECT search_reccommend.nickname AS peer, recommended_peer_nickname
FROM search_reccommend
JOIN search_max ON
search_reccommend.nickname = search_max.nickname
AND search_reccommend.count_rec = search_max.max_count;
END;

$$ LANGUAGE plpgsql;
----------------------------------------- TEST 3.8 -----------------------------------------
--BEGIN;
--CALL recommended_by_friends('ref');
--FETCH ALL IN "ref";
--END;
----------------------------------------- PART 3.9 -----------------------------------------
CREATE OR REPLACE PROCEDURE percentage_of_peers_that_started_blocks(
    blockname1 VARCHAR, blockname2 VARCHAR, IN REF refcursor
  ) AS $$ BEGIN OPEN REF FOR WITH block1 AS (
    SELECT DISTINCT peer_nickname
FROM Checks
WHERE task_name SIMILAR TO blockname1
  ), block2 AS (
    SELECT DISTINCT peer_nickname
FROM Checks
WHERE task_name SIMILAR TO blockname2
  ), both_blocks AS (
    SELECT DISTINCT peer_nickname
FROM block1
INTERSECT
    SELECT DISTINCT peer_nickname
FROM block2
  ), any_blocks AS (
    SELECT nickname AS peer_nickname
FROM Peers
EXCEPT (
        SELECT DISTINCT peer_nickname
FROM block1
UNION
        SELECT DISTINCT peer_nickname
FROM block2
      )
  )
SELECT(
    SELECT count(peer_nickname)
FROM block1
  ) * 100 / count(nickname) AS StartedBlock1,(
    SELECT count(peer_nickname)
FROM block2
  ) * 100 / count(nickname) AS StartedBlock2,(
    SELECT count(peer_nickname)
FROM both_blocks
  ) * 100 / count(nickname) AS StartedBothBlocks,(
    SELECT count(peer_nickname)
FROM any_blocks
  ) * 100 / count(nickname) AS DidntStartAnyBlock
FROM Peers;
END;

$$ LANGUAGE plpgsql;
----------------------------------------- TEST 3.9 -----------------------------------------
--BEGIN;
--CALL percentage_of_peers_that_started_blocks('A[0-9]%', 'CPP%', 'ref');
--FETCH ALL IN "ref";
--END;
----------------------------------------- PART 3.10 -----------------------------------------
CREATE OR REPLACE PROCEDURE percentage_of_checks_birthday(IN REF refcursor) AS $$ BEGIN OPEN REF FOR WITH birthday_p2p AS (
    SELECT id, peer_nickname
FROM checks
JOIN peers ON
peer_nickname = nickname
WHERE date_part('day', check_date) = date_part('day', birthday)
  AND date_part('month', check_date) = date_part('month', birthday)
  ), success_p2p AS (
    SELECT DISTINCT ON
(peer_nickname) birthday_p2p.id, peer_nickname, p2p_check_status
FROM birthday_p2p
JOIN p2p ON
birthday_p2p.id = p2p.check_id
WHERE p2p_check_status = 'Success'
  ), failure_p2p AS (
    SELECT DISTINCT ON
(peer_nickname) birthday_p2p.id, peer_nickname, p2p_check_status
FROM birthday_p2p
JOIN p2p ON
birthday_p2p.id = p2p.check_id
WHERE p2p_check_status = 'Failure'
  ), count_success AS (
    SELECT count(peer_nickname)
FROM success_p2p
  ), count_failure AS (
    SELECT count(peer_nickname)
FROM failure_p2p
  ), sum_success_failure AS (
    SELECT(
        SELECT *
FROM count_success
      ) + (
        SELECT *
FROM count_failure
      )
  )
SELECT(
    (
      SELECT *
FROM count_success
    )::NUMERIC(6, 3) / (
      SELECT *
FROM sum_success_failure
    )
  )::NUMERIC(6, 3) * 100 SuccessfulChecks,(
    (
      SELECT *
FROM count_failure
    )::NUMERIC(6, 3) / (
      SELECT *
FROM sum_success_failure
    )
  )::NUMERIC(6, 3) * 100 UnsuccessfulChecks
FROM count_failure;
END;

$$ LANGUAGE plpgsql;
----------------------------------------- TEST 3.10 -----------------------------------------
--BEGIN;
--CALL percentage_of_checks_birthday('ref');
--FETCH ALL IN "ref";
--END;
----------------------------------------- PART 3.11 -----------------------------------------
CREATE OR REPLACE PROCEDURE successful_and_not(
    task1 varchar, task2 varchar, task3 varchar, REF refcursor
  ) AS $$ BEGIN OPEN REF FOR WITH task_1 AS (
    SELECT peer
FROM get_user_xp_for_task()
WHERE task1 IN (
        SELECT task
FROM get_user_xp_for_task()
      )
  ), task_2 AS (
    SELECT peer
FROM get_user_xp_for_task()
WHERE task2 IN (
        SELECT task
FROM get_user_xp_for_task()
      )
  ), task_3 AS (
    SELECT peer
FROM get_user_xp_for_task()
WHERE task3 NOT IN (
        SELECT task
FROM get_user_xp_for_task()
      )
  )
SELECT *
FROM(
    (
      SELECT *
FROM task_1
    )
INTERSECT
    (
      SELECT *
FROM task_2
    )
INTERSECT
    (
      SELECT *
FROM task_3
    )
  ) AS new_table;
END;

$$ LANGUAGE plpgsql;
----------------------------------------- TEST 3.11 -----------------------------------------
--BEGIN;
--CALL successful_and_not('C2_s21_string+', 'C4_s21_decimal', 'D01_Linux', 'ref');
--FETCH ALL IN "ref";
--END;
----------------------------------------- PART 3.12 -----------------------------------------
CREATE OR REPLACE PROCEDURE previous_tasks(IN REF refcursor) AS $$ BEGIN OPEN REF FOR WITH RECURSIVE recursion AS (
    SELECT 'C2_s21_string+'::VARCHAR AS task_name, 0::BIGINT AS PrevCount
UNION
    SELECT name, recursion.PrevCount + 1
FROM recursion, Tasks
WHERE entry_condition = recursion.task_name
AND PrevCount < (
        SELECT count(*)
FROM Tasks
      )
  )
SELECT *
FROM recursion;
END;

$$ LANGUAGE plpgsql;
----------------------------------------- TEST 3.12 -----------------------------------------
--BEGIN;
--CALL previous_tasks('ref');
--FETCH ALL IN "ref";
--END;
----------------------------------------- PART 3.13 -----------------------------------------
CREATE OR replace PROCEDURE lucky_day(IN n int, IN REF refcursor) AS $$ BEGIN OPEN REF FOR WITH t AS (
    SELECT *
FROM checks
JOIN p2p ON
checks.id = p2p.check_id
LEFT JOIN verter ON
checks.id = verter.check_id
JOIN tasks ON
checks.task_name = tasks.name
JOIN xp ON
checks.id = xp.check_id
WHERE p2p.p2p_check_status = 'Success'
AND (
        verter.verter_check_status = 'Success'
  OR verter.verter_check_status IS NULL
      )
  )
SELECT check_date
FROM t
WHERE t.num_xp_received >= t.max_xp * 0.8
GROUP BY check_date
HAVING count(check_date) >= n;
END;

$$ LANGUAGE plpgsql;
----------------------------------------- TEST 3.13 -----------------------------------------
--BEGIN;
--CALL lucky_day(3, 'ref');
--FETCH ALL IN "ref";
--END;
----------------------------------------- PART 3.14 -----------------------------------------
CREATE OR REPLACE PROCEDURE max_peer_xp(IN REF refcursor) AS $$ BEGIN OPEN REF FOR
SELECT checks.peer_nickname, SUM(num_xp_received) AS XP
FROM xp
JOIN checks ON
xp.check_id = checks.id
GROUP BY checks.peer_nickname
ORDER BY XP DESC
LIMIT 1;
END;

$$ LANGUAGE plpgsql;
----------------------------------------- TEST 3.14 -----------------------------------------
--BEGIN;
--CALL max_peer_xp('ref');
--FETCH ALL IN "ref";
--END;
----------------------------------------- PART 3.15 -----------------------------------------
CREATE OR REPLACE PROCEDURE peer_came_early(IN "time" time, IN n integer, IN REF refcursor) AS $$ BEGIN OPEN REF FOR WITH tmp AS (
    SELECT TT.peer_nickname, count(*)
FROM TimeTracking AS TT
WHERE TT.state = 1
AND TT.time <= peer_came_early."time"
GROUP BY TT.peer_nickname
HAVING count(*) >= n
  )
SELECT peer_nickname
FROM tmp;
END;

$$ LANGUAGE plpgsql;
----------------------------------------- TEST 3.15 -----------------------------------------
--BEGIN;
--CALL peer_came_early('13:38:00', 2, 'ref');
--FETCH ALL IN "ref";
--END;
----------------------------------------- PART 3.16 -----------------------------------------
CREATE OR REPLACE PROCEDURE peers_leaving_campus(N int, M int, IN REF refcursor) AS $$ BEGIN OPEN REF FOR WITH all_out AS (
    SELECT *
FROM timetracking
WHERE state = 2
AND date >= (now() - (N - 1 || ' days')::INTERVAL)::date
  AND date <= now()::date
  )
SELECT peer_nickname AS "Peer list"
FROM all_out
GROUP BY peer_nickname
HAVING count(state) > M;
END $$ LANGUAGE plpgsql;
----------------------------------------- TEST 3.17 -----------------------------------------
--BEGIN;
--CALL peers_leaving_campus(600, 1, 'ref');
--FETCH ALL IN "ref";
--END;
----------------------------------------- PART 3.17 -----------------------------------------
CREATE OR REPLACE PROCEDURE percentage_of_early_entries(IN REF refcursor) AS $$ BEGIN OPEN REF FOR WITH months AS (
    SELECT date '2000-01-01' + INTERVAL '1' MONTH * s.a AS date
FROM generate_series(0, 11) AS s(a)
  ), person_in AS (
    SELECT TT.peer_nickname, TT.date, TT.time
FROM TimeTracking TT
WHERE state = 1
  )
SELECT to_char(m.date, 'Month') AS MONTH,(
    CASE
      WHEN count(peer_nickname) != 0 THEN (
        (
          count(peer_nickname) FILTER (
WHERE time < '12:00:00'
          ) / count(peer_nickname)::float
        ) * 100
      )::int
ELSE 0
END
  ) AS EarlyEntries
FROM months m
LEFT JOIN peers ON
to_char(m.date, 'Month') = to_char(birthday, 'Month')
LEFT JOIN person_in pi ON
peers.nickname = pi.peer_nickname
GROUP BY m.date
ORDER BY m.date;
END;

$$ LANGUAGE plpgsql;
----------------------------------------- TEST 3.17 -----------------------------------------
--BEGIN;
--CALL percentage_of_early_entries('ref');
--FETCH ALL IN "ref";
--END;