import time
import requests
import json
from dotenv import load_dotenv
import os
import mysql.connector
import random

#List of Friends
friends_list = [
    ('friend1', 'friend1_tagline')

    ]



#Get env stuff
load_dotenv()

api_key = os.getenv('api_key')
headers = {'X-Riot-Token': api_key}

host = os.getenv('host')
user = os.getenv('user')
password = os.getenv('password')
database = os.getenv('database')



#Database Connection
connection = mysql.connector.connect(
    host = host,
    user = user    ,
    password = password,
    database = database,
)
if connection.is_connected():
    print("Connected to MySQL database")
else:
    print("Connection failed")


    #Get puuid
def get_puuid(gameName, tagLine):
    url = f'https://americas.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{gameName}/{tagLine}'
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        return data.get('puuid')
    else:
        print(f"Failed to get puuid: {response.status_code}")





    #Get match IDs
def get_match_ids(puuid):
    match_ids = []
    queue_type = [400, 420, 440]

    for queue_type in queue_type:
        start = 0
        count = 100

        while True:
            url = f'https://americas.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids'
            params = {'count': count, 'start': start, 'queue': queue_type}
            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                batch = response.json()
                if not batch:
                    break

                match_ids.extend(batch)
                start += count
            elif response.status_code == 429:
                print('Rate limit hit, waiting before retrying...')
                time.sleep(30)
            else:
                print(f'Error fetching matches: {response.status_code}')
                break

    return match_ids






    #Get match data


def get_match_data(match_id):
    url = f'https://americas.api.riotgames.com/lol/match/v5/matches/{match_id}'
    attempt = 0
    max_attempts = 5  # Max retries for rate limit hits

    while attempt < max_attempts:
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 429:
            reset_time = response.headers.get('X-RateLimit-Reset')
            if reset_time:
                reset_time = int(reset_time)
                current_time = int(time.time())
                wait_time = reset_time - current_time + 1
                print(f"Rate limit hit. Waiting for {wait_time} seconds (Attempt {attempt}/{max_attempts})...")
                time.sleep(wait_time + random.uniform(0, 1))  # Add small random sleep to spread retries
            else:
                # Fallback if X-RateLimit-Reset header is missing
                wait_time = 120  # Explicitly wait for 2 minutes
                print(f"Rate limit error, no reset header. Retrying in {wait_time} seconds...")
                time.sleep(wait_time + random.uniform(0, 1))  # Add random jitter
        else:
            print(f"Error: Status code: {response.status_code}")
            return None

        attempt += 1

    print("Max retry attempts reached. Giving up.")
    return None




    #Check if match_id exists
def match_exists(match_id):
    global connection
    cursor = connection.cursor()
    query = "SELECT COUNT(*) FROM matches WHERE match_id = %s"
    cursor.execute(query,(match_id,))
    result = cursor.fetchone()
    cursor.close()
    return result[0]>0



    #Check if puuid exists for summoner table
def puuid_exists(puuid):
    global connection
    cursor = connection.cursor()
    query = "SELECT COUNT(*) FROM summoners WHERE PUUID = %s"
    cursor.execute(query, (puuid,))
    result = cursor.fetchone()
    print(f"Query result: {result}")
    cursor.close()
    return result[0] > 0


    #Adds a new 'friend' to my summoner table, only needed when adding someone to the list
def insert_summoner(puuid, game_name, tag_line):
    global connection
    cursor = connection.cursor()
    query = """
    INSERT INTO summoners (PUUID, riot_id_game_name, riot_id_tagline)
    VALUES (%s, %s, %s)
    """
    cursor.execute(query, (puuid, game_name, tag_line))
    connection.commit()
    cursor.close()






#Store match data in database
def store_match_data(match_data):
    global connection

    match_id = match_data["metadata"]["matchId"]
    print(f"Attempting to store match data for match ID: {match_id}")

    cursor = connection.cursor()

    try:



        #Matches Table
        data_version = match_data["metadata"]["dataVersion"]
        end_of_game_result = match_data["info"].get("endOfGameResult", None)
        game_creation = match_data["info"]["gameCreation"]
        game_duration = match_data["info"]["gameDuration"]
        game_end_timestamp = match_data["info"]["gameEndTimestamp"]
        game_id = match_data["info"]["gameId"]
        game_mode = match_data["info"]["gameMode"]
        game_name = match_data["info"]["gameName"]
        game_start_timestamp = match_data["info"]["gameStartTimestamp"]
        game_type = match_data["info"]["gameType"]
        game_version = match_data["info"]["gameVersion"]
        map_id = match_data["info"]["mapId"]
        platform_id = match_data["info"]["platformId"]
        queue_id = match_data["info"]["queueId"]

        cursor.execute("""
            insert into matches (match_id, data_version, end_of_game_result, game_creation, game_duration, game_end_timestamp, game_id, game_mode, game_name, game_start_timestamp, game_type, game_version, map_id, platform_id, queue_id)
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (match_id, data_version, end_of_game_result, game_creation,game_duration,game_end_timestamp, game_id, game_mode, game_name, game_start_timestamp, game_type, game_version, map_id, platform_id, queue_id))



        #Participants table
        for participant in match_data["info"]["participants"]:
            # Extracting necessary participant data
            puuid = participant["puuid"]
            all_in_pings = participant["allInPings"]
            assist_me_pings = participant["assistMePings"]
            assists = participant["assists"]
            baron_kills = participant["baronKills"]
            champ_level = participant["champLevel"]
            champion_name = participant["championName"]
            champion_id = participant["championId"]
            command_pings = participant["commandPings"]
            champion_transform = participant["championTransform"]
            consumables_purchased = participant["consumablesPurchased"]
            damage_dealt_to_buildings = participant["damageDealtToBuildings"]
            damage_dealt_to_objectives = participant["damageDealtToObjectives"]
            damage_dealt_to_turrets = participant["damageDealtToTurrets"]
            damage_self_mitigated = participant["damageSelfMitigated"]
            deaths = participant["deaths"]
            detector_wards_placed = participant["detectorWardsPlaced"]
            double_kills = participant["doubleKills"]
            dragon_kills = participant["dragonKills"]
            enemy_missing_pings = participant["enemyMissingPings"]
            enemy_vision_pings = participant["enemyVisionPings"]
            first_blood_assist = participant["firstBloodAssist"]
            first_blood_kill = participant["firstBloodKill"]
            first_tower_assist = participant["firstTowerAssist"]
            first_tower_kill = participant["firstTowerKill"]
            game_ended_in_early_surrender = participant["gameEndedInEarlySurrender"]
            game_ended_in_surrender = participant["gameEndedInSurrender"]
            hold_pings = participant["holdPings"]
            get_back_pings = participant["getBackPings"]
            gold_earned = participant["goldEarned"]
            gold_spent = participant["goldSpent"]
            individual_position = participant["individualPosition"]
            inhibitor_kills = participant["inhibitorKills"]
            inhibitor_takedowns = participant["inhibitorTakedowns"]
            inhibitors_lost = participant["inhibitorsLost"]
            item_0 = participant["item0"]
            item_1 = participant["item1"]
            item_2 = participant["item2"]
            item_3 = participant["item3"]
            item_4 = participant["item4"]
            item_5 = participant["item5"]
            item_6 = participant["item6"]
            items_purchased = participant["itemsPurchased"]
            killing_sprees = participant["killingSprees"]
            kills = participant["kills"]
            lane = participant["lane"]
            largest_critical_strike = participant["largestCriticalStrike"]
            largest_killing_spree = participant["largestKillingSpree"]
            largest_multi_kill = participant["largestMultiKill"]
            longest_time_spent_living = participant["longestTimeSpentLiving"]
            magic_damage_dealt = participant["magicDamageDealt"]
            magic_damage_dealt_to_champions = participant["magicDamageDealtToChampions"]
            magic_damage_taken = participant["magicDamageTaken"]
            neutral_minions_killed = participant["neutralMinionsKilled"]
            need_vision_pings = participant["needVisionPings"]
            nexus_kills = participant["nexusKills"]
            nexus_takedowns = participant["nexusTakedowns"]
            nexus_lost = participant["nexusLost"]
            objectives_stolen = participant["objectivesStolen"]
            objectives_stolen_assists = participant["objectivesStolenAssists"]
            on_my_way_pings = participant["onMyWayPings"]
            participant_id = participant["participantId"]
            penta_kills = participant["pentaKills"]
            physical_damage_dealt = participant["physicalDamageDealt"]
            physical_damage_dealt_to_champions = participant["physicalDamageDealtToChampions"]
            physical_damage_taken = participant["physicalDamageTaken"]
            push_pings = participant["pushPings"]
            quadra_kills = participant["quadraKills"]
            role = participant["role"]
            sight_wards_bought_in_game = participant["sightWardsBoughtInGame"]
            spell_1_casts = participant["spell1Casts"]
            spell_2_casts = participant["spell2Casts"]
            spell_3_casts = participant["spell3Casts"]
            spell_4_casts = participant["spell4Casts"]
            summoner_1_casts = participant["summoner1Casts"]
            summoner_1_id = participant["summoner1Id"]
            summoner_2_casts = participant["summoner2Casts"]
            summoner_2_id = participant["summoner2Id"]
            summoner_id = participant["summonerId"]
            summoner_level = participant["summonerLevel"]
            team_early_surrendered = participant["teamEarlySurrendered"]
            team_position = participant["teamPosition"]
            time_ccing_others = participant["timeCCingOthers"]
            time_played = participant["timePlayed"]
            total_ally_jungle_minions_killed = participant["totalAllyJungleMinionsKilled"]
            total_damage_dealt = participant["totalDamageDealt"]
            total_damage_dealt_to_champions = participant["totalDamageDealtToChampions"]
            total_damage_shielded_on_teammates = participant["totalDamageShieldedOnTeammates"]
            total_damage_taken = participant["totalDamageTaken"]
            total_enemy_jungle_minions_killed = participant["totalEnemyJungleMinionsKilled"]
            total_heal = participant["totalHeal"]
            total_heals_on_teammates = participant["totalHealsOnTeammates"]
            total_minions_killed = participant["totalMinionsKilled"]
            total_time_cc_dealt = participant["totalTimeCCDealt"]
            total_time_spent_dead = participant["totalTimeSpentDead"]
            total_units_healed = participant["totalUnitsHealed"]
            triple_kills = participant["tripleKills"]
            true_damage_dealt = participant["trueDamageDealt"]
            true_damage_dealt_to_champions = participant["trueDamageDealtToChampions"]
            true_damage_taken = participant["trueDamageTaken"]
            turret_kills = participant["turretKills"]
            turret_takedowns = participant["turretTakedowns"]
            turrets_lost = participant["turretsLost"]
            unreal_kills = participant["unrealKills"]
            vision_score = participant["visionScore"]
            vision_cleared_pings = participant["visionClearedPings"]
            vision_wards_bought_in_game = participant["visionWardsBoughtInGame"]
            wards_killed = participant["wardsKilled"]
            wards_placed = participant["wardsPlaced"]
            win = participant["win"]
            match_id = match_data["metadata"]["matchId"]
            team_id = participant["teamId"]

            cursor.execute("""
                insert into participants(match_id, puuid, team_id, all_in_pings, assist_me_pings, assists, baron_kills, champ_level, champion_name, champion_id, command_pings, champion_transform, consumables_purchased, damage_dealt_to_buildings, damage_dealt_to_objectives, damage_dealt_to_turrets, damage_self_mitigated, deaths, detector_wards_placed, double_kills, dragon_kills, enemy_missing_pings, enemy_vision_pings, first_blood_assist, first_blood_kill, first_tower_assist, first_tower_kill, game_ended_in_early_surrender, game_ended_in_surrender, hold_pings, get_back_pings, gold_earned, gold_spent, individual_position, inhibitor_kills, inhibitor_takedowns, inhibitors_lost, item_0, item_1, item_2, item_3, item_4, item_5, item_6, items_purchased, killing_sprees, kills, lane, largest_critical_strike, largest_killing_spree, largest_multi_kill, longest_time_spent_living, magic_damage_dealt, magic_damage_dealt_to_champions, magic_damage_taken, neutral_minions_killed, need_vision_pings, nexus_kills, nexus_takedowns, nexus_lost, objectives_stolen, objectives_stolen_assists, on_my_way_pings, participant_id, penta_kills, physical_damage_dealt, physical_damage_dealt_to_champions, physical_damage_taken, push_pings, quadra_kills, role, sight_wards_bought_in_game, spell_1_casts, spell_2_casts, spell_3_casts, spell_4_casts, summoner_1_casts, summoner_1_id, summoner_2_casts, summoner_2_id, summoner_id, summoner_level, team_early_surrendered, team_position, time_ccing_others, time_played, total_ally_jungle_minions_killed, total_damage_dealt, total_damage_dealt_to_champions, total_damage_shielded_on_teammates, total_damage_taken, total_enemy_jungle_minions_killed, total_heal, total_heals_on_teammates, total_minions_killed, total_time_cc_dealt, total_time_spent_dead, total_units_healed, triple_kills, true_damage_dealt, true_damage_dealt_to_champions, true_damage_taken, turret_kills, turret_takedowns, turrets_lost, unreal_kills, vision_score, vision_cleared_pings, vision_wards_bought_in_game, wards_killed, wards_placed, win)
                values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (match_id, puuid, team_id, all_in_pings, assist_me_pings, assists, baron_kills, champ_level, champion_name,
                  champion_id, command_pings, champion_transform, consumables_purchased, damage_dealt_to_buildings,
                  damage_dealt_to_objectives, damage_dealt_to_turrets, damage_self_mitigated, deaths, detector_wards_placed,
                  double_kills, dragon_kills, enemy_missing_pings, enemy_vision_pings, first_blood_assist, first_blood_kill,
                  first_tower_assist, first_tower_kill, game_ended_in_early_surrender, game_ended_in_surrender, hold_pings,
                  get_back_pings, gold_earned, gold_spent, individual_position, inhibitor_kills, inhibitor_takedowns,
                  inhibitors_lost, item_0, item_1, item_2, item_3, item_4, item_5, item_6, items_purchased, killing_sprees,
                  kills, lane, largest_critical_strike, largest_killing_spree, largest_multi_kill,
                  longest_time_spent_living, magic_damage_dealt, magic_damage_dealt_to_champions, magic_damage_taken,
                  neutral_minions_killed, need_vision_pings, nexus_kills, nexus_takedowns, nexus_lost, objectives_stolen,
                  objectives_stolen_assists, on_my_way_pings, participant_id, penta_kills, physical_damage_dealt,
                  physical_damage_dealt_to_champions, physical_damage_taken, push_pings, quadra_kills, role,
                  sight_wards_bought_in_game, spell_1_casts, spell_2_casts, spell_3_casts, spell_4_casts, summoner_1_casts,
                  summoner_1_id, summoner_2_casts, summoner_2_id, summoner_id, summoner_level, team_early_surrendered,
                  team_position, time_ccing_others, time_played, total_ally_jungle_minions_killed, total_damage_dealt,
                  total_damage_dealt_to_champions, total_damage_shielded_on_teammates, total_damage_taken,
                  total_enemy_jungle_minions_killed, total_heal, total_heals_on_teammates, total_minions_killed,
                  total_time_cc_dealt, total_time_spent_dead, total_units_healed, triple_kills, true_damage_dealt,
                  true_damage_dealt_to_champions, true_damage_taken, turret_kills, turret_takedowns, turrets_lost,
                  unreal_kills, vision_score, vision_cleared_pings, vision_wards_bought_in_game, wards_killed, wards_placed,
                  win))



            # Teams Table
        for team in match_data["info"]["teams"]:
            horde_data = team["objectives"].get("horde")
            team_id = team["teamId"]
            win = team["win"]
            baron_first = team["objectives"]["baron"]["first"]
            baron_kills = team["objectives"]["baron"]["kills"]
            champion_first = team["objectives"]["champion"]["first"]
            champion_kills = team["objectives"]["champion"]["kills"]
            dragon_first = team["objectives"]["dragon"]["first"]
            dragon_kills = team["objectives"]["dragon"]["kills"]
            horde_first = horde_data["first"] if horde_data else None
            horde_kills = horde_data["kills"] if horde_data else None
            inhibitor_first = team["objectives"]["inhibitor"]["first"]
            inhibitor_kills = team["objectives"]["inhibitor"]["kills"]
            rift_herald_first = team["objectives"]["riftHerald"]["first"]
            rift_herald_kills = team["objectives"]["riftHerald"]["kills"]
            tower_first = team["objectives"]["tower"]["first"]
            tower_kills = team["objectives"]["tower"]["kills"]

            cursor.execute("""
                insert into teams(match_id, team_id, win, baron_first, baron_kills, champion_first, champion_kills, dragon_first, dragon_kills, horde_first, horde_kills, inhibitor_first, inhibitor_kills, rift_herald_first, rift_herald_kills, tower_first, tower_kills)
                values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (match_id, team_id, win, baron_first, baron_kills, champion_first, champion_kills, dragon_first, dragon_kills, horde_first, horde_kills, inhibitor_first, inhibitor_kills, rift_herald_first, rift_herald_kills, tower_first, tower_kills))






        # Bans Table
        for team in match_data["info"]["teams"]:
            team_id = team["teamId"]
            for ban in team["bans"]:
                pick_turn = ban["pickTurn"]
                champion_id = ban["championId"]

                cursor.execute(
                    """
                    INSERT INTO bans (match_id, team_id, pick_turn, champion_id)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (match_id, team_id, pick_turn, champion_id)
                )

        connection.commit()
        print(f"Match {match_id} stored successfully")


    except Exception as e:
        connection.rollback()
        print(f"Error storing match data for {match_id}: {e}")


    finally:
        cursor.close()




# Function Calls
def main():
    for gameName, tagLine in friends_list:
        print(f"Processing {gameName}#{tagLine}...")  # Logging the summoner
        puuid = get_puuid(gameName, tagLine)
        if not puuid:
            print(f"Skipping {gameName}#{tagLine} due to missing PUUID.")
            continue

        # Check if puuid already exists in the database
        if not puuid_exists(puuid):
            print(f"Adding {gameName}#{tagLine} to summoners table...")
            insert_summoner(puuid, gameName, tagLine)

        print(f"Fetching match IDs for {gameName}#{tagLine}...")
        match_ids = get_match_ids(puuid)
        print(f"Found {len(match_ids)} matches for {gameName}#{tagLine}.")

        # Get the new matches (those that aren't in the database)
        new_match_ids = [match_id for match_id in match_ids if not match_exists(match_id)]

        # Calculate the number of preexisting matches
        preexisting_matches = len(set(match_ids) - set(new_match_ids))
        print(f"Found {preexisting_matches} pre-existing matches for {gameName}#{tagLine}.")
        print(f"Attempting to import {len(new_match_ids)} new matches.")

        # Process each new match
        for match_id in new_match_ids:
            match_data = get_match_data(match_id)
            if match_data:
                store_match_data(match_data)





if __name__ == '__main__':
    main()
