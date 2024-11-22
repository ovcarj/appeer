import click

from appeer.db.jobs_db import JobsDB

@click.command('pdb', help="""Print parse jobs summary

        To view a summary of all parse jobs, use:

        appeer pdb

        To see details of a job with a particular label:

        appeer pdb <label>

        To print all not yet committed parsed publications:

        appeer pdb -u

        Instructions for cleaning the parse database:

        appeer clean pjob --help

        """)
@click.option('-u', '--uncommitted', is_flag=True, default=False, help='Print not yet committed entries ')
@click.argument('label', nargs=1, required=False)
def pdb_cli(label, uncommitted):
    """
    Print parse jobs summary

    """

    jobs_db = JobsDB()

    if uncommitted:
        pass
#        jobs_db.print_all_uncommitted()

    elif label:
        pass
#        jobs_db.print_parse_job_details(label)

    else:
        jobs_db.print_parse_jobs()

def main():
    pdb_cli()

if __name__ == '__main__':
    main()
