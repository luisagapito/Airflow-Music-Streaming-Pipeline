from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    copy_sql = """
        INSERT INTO {}
        (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
        SELECT
                events.start_time, 
                events.userid, 
                events.level, 
                case when songs.song_id is null then 'none' else songs.song_id end as song_id, 
                case when songs.artist_id is null then 'none' else songs.artist_id end as artist_id,
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM {}
            WHERE page='NextSong') events
            LEFT JOIN {} songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """


    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 fact_table="",
                 staging_events="",
                 staging_songs="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.fact_table = fact_table
        self.staging_events = staging_events
        self.staging_songs = staging_songs


    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        #For Fact table only inserts data as suggessted in the project
        self.log.info("Inserting data to fact table")
        formatted_sql = LoadFactOperator.copy_sql.format(
            self.fact_table,
            self.staging_events,
            self.staging_songs
        )
        redshift.run(formatted_sql)
        
        
        
        
        
        
