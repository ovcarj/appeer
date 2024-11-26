import click

from collections import namedtuple

import appeer.log

from appeer.db.db import DB
#from appeer.scrape.scrape_plan import _get_allowed_strategies

class JobsDB(DB):
    """
    Handles the ``scrape.db`` database.

    """

    def __init__(self):
        """
        If the scrape database exists, establishes a connection and a cursor.

        """

        super().__init__(db_type='jobs')

        # TODO: strategies in the ScrapePlan will be revised
        #self._allowed_strategies = _get_allowed_strategies()

    def _define_scrape_job_tuple(self):
        """
        Defines the ``self._ScrapeJob`` named tuple, whose attributes have the same name as the columns in the ``scrape_jobs`` table.

        """

        self._ScrapeJob = namedtuple('ScrapeJob',
                """label,
                description,
                log,
                download_directory,
                zip_file,
                date,
                job_status,
                job_successes,
                job_fails,
                no_of_publications,
                job_parsed""")

    def _define_scrape_tuple(self):
        """
        Defines the ``self._Scrape`` named tuple, whose attributes have the same name as the columns in the ``scrape`` table.

        """

        self._Scrape = namedtuple('Scrape',
                """label,
                scrape_index,
                url,
                strategy,
                status,
                out_file,
                parsed""")

    def _set_scrape_job_factory(self):
        """
        Make rows returned by the cursor be ``self._ScrapeJob`` instances.

        """

        self._define_scrape_job_tuple()

        def scrape_job_factory(cursor, row):
            return self._ScrapeJob(*row)

        self._con.row_factory = scrape_job_factory
        self._cur = self._con.cursor()

    def _set_scrape_factory(self):
        """
        Make rows returned by the cursor be ``self._Scrape`` instances.

        """

        self._define_scrape_tuple()

        def scrape_factory(cursor, row):
            return self._Scrape(*row)

        self._con.row_factory = scrape_factory
        self._cur = self._con.cursor()

    def _define_parse_job_tuple(self):
        """
        Defines the ``self._ParseJob`` named tuple, whose attributes have the same name as the columns in the ``parse_jobs`` table.

        """

        self._ParseJob = namedtuple('ParseJob',
                """label,
                description,
                log,
                mode,
                parse_directory,
                date,
                job_status,
                job_successes,
                job_fails,
                no_of_publications,
                job_committed""")

    def _set_parse_job_factory(self):
        """
        Make rows returned by the cursor be ``self._ScrapeJob`` instances.

        """

        self._define_parse_job_tuple()

        def parse_job_factory(cursor, row):
            return self._ParseJob(*row)

        self._con.row_factory = parse_job_factory
        self._cur = self._con.cursor()

    def _initialize_database(self):
        """
        Initializes the SQL tables when the ``jobs`` database is created.

        """

        self._cur.execute('CREATE TABLE scrape_jobs(label, description, log, download_directory, zip_file, date, job_status, job_successes, job_fails, no_of_publications, job_parsed)')

        self._cur.execute('CREATE TABLE scrape(label, scrape_index, url, strategy, status, out_file, parsed)')

        self._cur.execute('CREATE TABLE parse_jobs(label, description, log, mode, parse_directory, date, job_status, job_successes, job_fails, no_of_publications, job_committed)')

        self._con.commit()

    def _add_scrape_job(self,
            label,
            description,
            log,
            download_directory,
            zip_file,
            date):
        """
        Initializes an entry for a scrape job.

        Parameters
        ----------
        label : str
            Label of the scrape job
        description : str
            Description of the scrape job
        log : str
            Path to the log of the scrape job
        download_directory : str
            Path to the directory where the data is being downloaded
        zip_file : str
            Path to the output ZIP file of the scrape job
        date : str
            Date on which the scrape job was initialized

        """

        data = ({
            'label': label,
            'description': description,
            'log': log,
            'download_directory': download_directory,
            'zip_file': zip_file,
            'date': date,
            'job_status': 'I',
            'job_successes': 0,
            'job_fails': 0,
            'no_of_publications': 0,
            'job_parsed': 'F'
            })

        self._cur.execute("""
        INSERT INTO scrape_jobs VALUES(:label, :description, :log, :download_directory, :zip_file, :date, :job_status, :job_successes, :job_fails, :no_of_publications, :job_parsed)
        """, data)

        self._con.commit()

    def _update_scrape_job_entry(self, label, column_name, new_value):
        """
        Given a ``label``, updates the corresponding ``column_name`` value with ``new_value`` in the ``scrape_jobs`` table.

        ``column_name`` must be in ['job_status', 'job_successes', 'job_fails', 'no_of publications', 'job_parsed'].

        Parameters
        ----------

        label : str
            Label of the scrape job that is being updated
        column_name : str
            Name of the column whose value is being updated
        new_value : str | int
            New value of the given column

        """

        match column_name:

            case 'job_status':

                if new_value not in ['I', 'R', 'X', 'E']:
                    raise ValueError(f'Cannot update the scrape database. Invalid job_status={new_value} given; must be "I", "R", "X" or "E".')
        
                else:
        
                    self._cur.execute("""
                    UPDATE scrape_jobs SET job_status = ? WHERE label = ?
                    """, (new_value, label))
        
                    self._con.commit()

            case 'no_of_publications':

                if not isinstance(new_value, int):
                    raise ValueError(f'Cannot update the scrape database. Invalid no_of_publications={new_value} given; must be a positive integer.')
        
                elif not (new_value > 0):
                    raise ValueError(f'Cannot update the scrape database. Invalid no_of_publications={new_value} given; must be a positive integer.')

                else:
        
                    self._cur.execute("""
                    UPDATE scrape_jobs SET no_of_publications = ? WHERE label = ?
                    """, (new_value, label))
        
                    self._con.commit()

            case 'job_successes':

                if not isinstance(new_value, int):
                    raise ValueError(f'Cannot update the scrape database. Invalid no_of_success={new_value} given; must be an integer.')
        
                elif not (new_value >= 0):
                    raise ValueError(f'Cannot update the scrape database. Invalid no_of_success={new_value} given; must be a non-negative integer.')

                else:
        
                    self._cur.execute("""
                    UPDATE scrape_jobs SET job_successes = ? WHERE label = ?
                    """, (new_value, label))
        
                    self._con.commit()

            case 'job_fails':

                if not isinstance(new_value, int):
                    raise ValueError(f'Cannot update the scrape database. Invalid no_of_fails={new_value} given; must be an integer.')
        
                elif not (new_value >= 0):
                    raise ValueError(f'Cannot update the scrape database. Invalid no_of_fails={new_value} given; must be a non-negative integer.')

                else:
        
                    self._cur.execute("""
                    UPDATE scrape_jobs SET job_fails = ? WHERE label = ?
                    """, (new_value, label))
        
                    self._con.commit()

            case 'job_parsed':

                if new_value not in ['T', 'F']:
                    raise ValueError(f'Cannot update the scrape database. Invalid job_parsed={new_value} given; must be "T" or "F".')
        
                else:
        
                    self._cur.execute("""
                    UPDATE scrape_jobs SET job_parsed = ? WHERE label = ?
                    """, (new_value, label))
        
                    self._con.commit()

            case _:

                raise ValueError(f'Cannot update the scrape database. Invalid column name {column_name} given.')

    def _get_scrape_jobs(self):
        """
        Stores the data from the ``scrape_jobs`` table to the ``self.scrape_jobs`` list. 
        
        Each element of the list is a named tuple with attributes with the same name as the ``scrape_jobs`` column names.

        """

        self._set_scrape_job_factory()

        self._cur.execute("""
        SELECT * FROM scrape_jobs
        """)

        self.scrape_jobs = self._cur.fetchall()

    def print_scrape_jobs(self):
        """
        Prints a summary of all entries in the ``scrape_jobs`` table.

        """
        
        self._get_scrape_jobs()

        header = '{:30s} {:35s} {:^4s} {:^4s} {:^10s}'.format('Label', 'Description', 'S', 'P', 'Succ./Tot.')
        header_length = len(header)
        dashes = header_length * '–'

        click.echo(dashes)
        click.echo(header)
        click.echo(dashes)

        for i, job in enumerate(self.scrape_jobs):

            description = job.description

            if len(description) > 30:
                description = description[0:30] + '...'

            succ_tot = f'{job.job_successes}/{job.no_of_publications}'

            report = '{:30s} {:35s} {:^4s} {:^4s} {:^10s}'.format(job.label, description, job.job_status, job.job_parsed, succ_tot)

            click.echo(report)

        click.echo(dashes)

        click.echo('S = Scrape job status: (I) Initialized; (R) Running; (X) Executed/Finished; (E) Error')
        click.echo('P = Scrape job completely parsed: (T) True; (F) False')
        click.echo('Succ./Tot. = Ratio of successful scrapes over total inputted URLs')

        click.echo(dashes)

    def _get_scrape_job(self, scrape_label):
        """
        Returns an instance of the ``self._ScrapeJob`` named tuple for a scrape job with label ``scrape_label``. 
        
        Parameters
        ----------
        scrape_label | str
            Label of the sought scrape job

        """

        self._set_scrape_job_factory()

        self._cur.execute("""
        SELECT * FROM scrape_jobs WHERE label = ?
        """, (scrape_label,))

        scrape_job = self._cur.fetchone()

        return scrape_job

    def _scrape_job_exists(self, scrape_label):
        """
        Checks whether the scrape job with ``scrape_label`` exists in the database.

        Parameters
        ----------
        scrape_label | str
            Label of the scrape job whose existence is being checked

        Returns
        -------
        job_exists | bool
            True if scrape job exists, False if it does not

        """

        job = self._get_scrape_job(scrape_label=scrape_label)

        if job is None:
            job_exists = False

        else:
            job_exists = True

        return job_exists

    def _get_bad_scrape_jobs(self):
        """
        Returns all scrape jobs whose status is not 'X'.

        Returns
        -------
        bad_jobs | list
            List of ScrapeJob instances for which the job status is not 'X'

        """

        self._set_scrape_job_factory()

        self._cur.execute("""
        SELECT * FROM scrape_jobs WHERE job_status!='X'
        """)

        bad_jobs = self._cur.fetchall()

        return bad_jobs

    def delete_job_entry(self, scrape_label):
        """
        Deletes the row given by ``scrape_label`` from the ``scrape_jobs`` table.

        Parameters
        ----------
        scrape_label | str
            Label of the scrape job whose entry is being removed

        Returns
        -------
        success | bool
            True if the entry was removed, False if it was not

        """

        click.echo(f'Removing entry {scrape_label} from the scrape database ...')

        job_exists = self._scrape_job_exists(scrape_label)

        if not job_exists:
            click.echo(f'The entry for scrape job {scrape_label} does not exist.')
            success = False

        else:
            
            self._cur.execute("""
            DELETE FROM scrape_jobs WHERE label = ?
            """, (scrape_label,))

            self._con.commit()

            job_exists = self._scrape_job_exists(scrape_label)

            if not job_exists:

                self._cur.execute("""
                DELETE FROM scrape WHERE label = ?
                """, (scrape_label,))

                self._con.commit()

                click.echo(f'Entry {scrape_label} removed.\n')
                success = True

            else:
                click.echo(f'Could not delete entry {scrape_label}\n')
                success = False

        return success

    def _add_scrape(self,
            label,
            scrape_index,
            url,
            strategy):
        """
        Initializes an entry for a scrape.

        Parameters
        ----------
        label : str
            Label of the scrape job
        scrape_index : int
            Index of the URL in the input
        url : str
            Inputted URL
        strategy : str
            Strategy used for scraping

        """

        data = ({
            'label': label,
            'scrape_index': scrape_index,
            'url': url,
            'strategy': strategy,
            'status': 'I',
            'out_file': 'no_file',
            'parsed': 'F',
            })

        self._cur.execute("""
        INSERT INTO scrape VALUES(:label, :scrape_index, :url, :strategy, :status, :out_file, :parsed)
        """, data)

        self._con.commit()

    def _update_scrape_entry(self, label, scrape_index, column_name, new_value):
        """
        Given a ``label`` and ``scrape_index``, updates the corresponding ``column_name`` value with ``new_value`` in the ``scrape`` table.

        ``column_name`` must be in ['strategy', 'status', 'out_file', 'parsed'].

        Parameters
        ----------
        label : str
            Label of the job that the scrape belongs to
        scrape_index : int
            Index of the URL in the input
        column_name : str
            Name of the column whose value is being updated
        new_value : str | int
            New value of the given column

        """

        match column_name:

            case 'strategy':

                # TODO: strategies in the ScrapePlan will be revised
                # if False is left as a placeholder
                if False:
                    raise ValueError(f'Cannot update the scrape database. Invalid strategy={new_value} given; must be ...')
        
                else:
        
                    self._cur.execute("""
                    UPDATE scrape SET strategy = ? WHERE label = ? AND scrape_index = ?
                    """, (new_value, label, scrape_index))
        
                    self._con.commit()

            case 'status':

                if not new_value in ['I', 'R', 'E', 'X']:
                    raise ValueError(f'Cannot update the scrape database. Invalid status={new_value} given; must be one of ["I", "R", "E", "X"].')
        
                else:
        
                    self._cur.execute("""
                    UPDATE scrape SET status = ? WHERE label = ? AND scrape_index = ?
                    """, (new_value, label, scrape_index))
        
                    self._con.commit()

            case 'out_file':

                if not isinstance(new_value, str):
                    raise ValueError(f'Cannot update the scrape database. Invalid out_file={new_value} given; must be a string.')
        
                else:
        
                    self._cur.execute("""
                    UPDATE scrape SET out_file = ? WHERE label = ? AND scrape_index = ?
                    """, (new_value, label, scrape_index))
        
                    self._con.commit()

            case 'parsed':

                if new_value not in ['T', 'F']:
                    raise ValueError(f'Cannot update the scrape database. Invalid parsed={new_value} given; must be "T" or "F".')
        
                else:
        
                    self._cur.execute("""
                    UPDATE scrape SET parsed = ? WHERE label = ? AND scrape_index = ?
                    """, (new_value, label, scrape_index))
        
                    self._con.commit()

            case _:

                raise ValueError(f'Cannot update the scrape database. Invalid column name {column_name} given.')

    def _get_scrapes(self, label):
        """
        Returns all scrapes for a given job label.

        Parameters
        ----------
        label | str
            Label of the job for which the scrapes are returned

        Returns
        -------
        scrapes | list
            List of Scrape instances for the given label

        """

        job_exists = self._scrape_job_exists(label)

        if not job_exists:
            click.echo(f'Scrape job {label} does not exist.')

        else:
            self._set_scrape_factory()

            self._cur.execute("""
            SELECT * FROM scrape WHERE label = ?
            """, (label,))

            scrapes = self._cur.fetchall()

        return scrapes

    def _get_all_unparsed(self):
        """
        Returns all scrapes with 'X' status and 'F' parse flag (all unparsed scrapes).

        """

        self._set_scrape_factory()

        self._cur.execute("""
        SELECT * FROM scrape WHERE status = ? AND parsed = ?
        """, ('X', 'F'))

        scrapes = self._cur.fetchall()

        return scrapes

    def print_scrape_job_details(self, label):
        """
        Prints details of the job with the given label.

        Parameters
        ----------
        label | str
            Label of the job for which the details are printed

        """

        job_exists = self._scrape_job_exists(label)

        if not job_exists:
            click.echo(f'Scrape job {label} does not exist.')

        else:

            dashes = appeer.log.get_log_dashes()

            job = self._get_scrape_job(label)

            click.echo(appeer.log.boxed_message(f'SCRAPE JOB: {job.label}'))
            click.echo(job.description)
            click.echo(f'Date: {job.date}')
            click.echo(dashes)
            click.echo('{:19s} {:s}'.format('Log', job.log))
            click.echo('{:19s} {:s}'.format('Download directory', job.download_directory))
            click.echo('{:19s} {:s}'.format('Output ZIP file', job.zip_file))
            click.echo(dashes)
            click.echo('{:19s} {:s}'.format('Job status', job.job_status))
            click.echo('{:19s} {:d}/{:d}'.format('Succ./Tot.', job.job_successes, job.no_of_publications))
            click.echo('{:19s} {:s}'.format('Completely parsed', job.job_parsed))
            click.echo(dashes)

            scrapes = self._get_scrapes(label)

            if not scrapes:
                click.echo('No files downloaded')

            else:
                click.echo(appeer.log.boxed_message('SCRAPE DETAILS'))

                header = '{:<10s} {:<10s} {:^4s} {:^8s} {:<16s} {:<80s}'.format('Index', 'Strategy', 'S', 'P', 'Output', 'URL')
                dashes_details = len(header) * '–'

                click.echo(dashes_details)
                click.echo(header)
                click.echo(dashes_details)

                for scrape in scrapes:
                    click.echo('{:<10d} {:<10s} {:^4s} {:^8s} {:<16s} {:<80s}'.format(scrape.scrape_index, scrape.strategy, scrape.status, scrape.parsed, scrape.out_file, scrape.url))

                click.echo(dashes_details)

                click.echo('S = Scrape status: (I) Initialized; (R) Running; (X) Executed/Finished; (E) Error')
                click.echo('P = Scrape parsed: (T) True; (F) False')
                click.echo(dashes_details)

    def print_all_unparsed(self):
        """
        Prints all unparsed scrapes.

        """

        scrapes = self._get_all_unparsed()

        if scrapes:

            header = '{:<30s} {:<6s} {:<70s} {:<65s}'.format('Label', 'Index', 'ZIP', 'URL')
            dashes_details = len(header) * '–'
     
            click.echo(dashes_details)
            click.echo(header)
            click.echo(dashes_details)
    
            for scrape in scrapes:
    
                job = self._get_scrape_job(scrape.label)
                zip_file = job.zip_file
    
                click.echo('{:<30s} {:<6d} {:<70s} {:<65s}'.format(scrape.label, scrape.scrape_index, zip_file, scrape.url))
    
            click.echo(dashes_details)

        else:
            click.echo('No unparsed scrapes found.')

    def _add_parse_job(self,
            label,
            description,
            log,
            mode,
            parse_directory,
            date):
        """
        Initializes an entry for a parse job.

        Parameters
        ----------
        label : str
            Label of the parse job
        description : str
            Description of the parse job
        log : str
            Path to the log of the parse job
        mode : str
            Parsing mode. Must be in ['A', 'S', 'F', 'E']
        parse_directory : str
            Path to the directory where (temporary) files for parsing are created
        date : str
            Date on which the parse job was initialized

        """

        if mode not in ['A', 'S', 'F', 'E']:
            raise ValueError('Cannot add entry to the parse jobs database; invalid mode {mode} inputted.')

        else:

            data = ({
                'label': label,
                'description': description,
                'log': log,
                'mode': mode,
                'parse_directory': parse_directory,
                'date': date,
                'job_status': 'I',
                'job_successes': 0,
                'job_fails': 0,
                'no_of_publications': 0,
                'job_committed': 'F'
                })
    
            self._cur.execute("""
            INSERT INTO parse_jobs VALUES(:label, :description, :log, :mode, :parse_directory, :date, :job_status, :job_successes, :job_fails, :no_of_publications, :job_committed)
            """, data)
    
            self._con.commit()

    def _get_parse_jobs(self):
        """
        Stores the data from the ``parse_jobs`` table to the ``self.parse_jobs`` list. 
        
        Each element of the list is a named tuple with attributes with the same name as the ``parse_jobs`` column names.

        """

        self._set_parse_job_factory()

        self._cur.execute("""
        SELECT * FROM parse_jobs
        """)

        self.parse_jobs = self._cur.fetchall()

    def print_parse_jobs(self):
        """
        Prints a summary of all entries in the ``parse_jobs`` table.

        """
        
        self._get_parse_jobs()

        header = '{:30s} {:25s} {:^4s} {:^4s} {:^4s} {:^10s}'.format('Label', 'Description', 'M', 'S', 'C', 'Succ./Tot.')
        header_length = len(header)
        dashes = header_length * '–'

        click.echo(dashes)
        click.echo(header)
        click.echo(dashes)

        for i, job in enumerate(self.parse_jobs):

            description = job.description

            if len(description) > 20:
                description = description[0:20] + '...'

            succ_tot = f'{job.job_successes}/{job.no_of_publications}'

            report = '{:30s} {:25s} {:^4s} {:^4s} {:^4s} {:^10s}'.format(job.label, description, job.mode, job.job_status, job.job_committed, succ_tot)

            click.echo(report)

        click.echo(dashes)

        click.echo('M = Parse job mode: (A) Auto; (E) Everything; (S) Scrape job; (F) File list')
        click.echo('S = Parse job status: (I) Initialized; (R) Running; (X) Executed/Finished; (E) Error')
        click.echo('C = Parse job committed: (T) True; (F) False')
        click.echo('Succ./Tot. = Ratio of successful parsed publications over total inputted publications')

        click.echo(dashes)

    def _get_parse_job(self, parse_label):
        """
        Returns an instance of the ``self._ParseJob`` named tuple for a parse job with label ``parse_label``. 
        
        Parameters
        ----------
        parse_label | str
            Label of the sought parse job

        """

        self._set_parse_job_factory()

        self._cur.execute("""
        SELECT * FROM parse_jobs WHERE label = ?
        """, (parse_label,))

        parse_job = self._cur.fetchone()

        return parse_job

    def _parse_job_exists(self, parse_label):
        """
        Checks whether the parse job with ``parse_label`` exists in the database.

        Parameters
        ----------
        parse_label | str
            Label of the parse job whose existence is being checked

        Returns
        -------
        job_exists | bool
            True if parse job exists, False if it does not

        """

        job = self._get_parse_job(parse_label=parse_label)

        if job is None:
            job_exists = False

        else:
            job_exists = True

        return job_exists

    def delete_parse_job_entry(self, parse_label):
        """
        Deletes the row given by ``parse_label`` from the ``parse_jobs`` table.

        Parameters
        ----------
        parse_label | str
            Label of the parse job whose entry is being removed

        Returns
        -------
        success | bool
            True if the entry was removed, False if it was not

        """

        click.echo(f'Removing entry {parse_label} from the parse database ...')

        job_exists = self._parse_job_exists(parse_label)

        if not job_exists:
            click.echo(f'The entry for parse job {parse_label} does not exist.')
            success = False

        else:
            
            self._cur.execute("""
            DELETE FROM parse_jobs WHERE label = ?
            """, (parse_label,))

            self._con.commit()

            job_exists = self._parse_job_exists(parse_label)

            if not job_exists:
                success = True

#                self._cur.execute("""
#                DELETE FROM parse WHERE label = ?
#                """, (parse_label,))

#                self._con.commit()

#                click.echo(f'Entry {parse_label} removed.\n')
#                success = True

            else:
                click.echo(f'Could not delete entry {parse_label}\n')
                success = False

        return success
