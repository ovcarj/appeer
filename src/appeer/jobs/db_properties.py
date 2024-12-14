"""Descriptor mechanism to get/set values in the database through attributes"""

class DBProperty:
    """
    Get/set a value in the database through Job attributes

    """

    def __init__(self, name):

        self.name = name

    def __get__(self, instance, owner):

        if instance._job_exists:
            _value = getattr(instance._job_entry, self.name)

        else:
            _value = None

        return _value

    def __set__(self, instance, val):

        if instance._job_mode == 'read':
            raise PermissionError(f'Cannot modify the {self.name} attribute in job_mode="read".')

        if instance._job_mode == 'write':

            if instance._job_exists:

                match instance._job_type:

                    case 'scrape_job':
                        instance._db.scrape_jobs.update_entry(
                                label=instance.label,
                                column_name=self.name,
                                new_value=val)

                    case 'parse_job':
                        instance._db.parse_jobs.update_entry(
                                label=instance.label,
                                column_name=self.name,
                                new_value=val)

                    case 'commit_job':

                        instance._db.commit_jobs.update_entry(
                                label=instance.label,
                                column_name=self.name,
                                new_value=val)
            else:
                raise PermissionError(f'Cannot modify "{self.name}"; the job with the label "{instance.label}" does not exist.')


