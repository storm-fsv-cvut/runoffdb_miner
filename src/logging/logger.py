class RunLogger:
    def __init__(self, log_file_path):
        self.log_file_path = log_file_path
        self.run_log = {}
    def log(self, run_id, text_to_log):
        """
        Creates run id indexed log
        :param run_id: run ID as key
        :param text_to_log: text to append to the key
        :return: None
        """
        if run_id not in self.run_log.keys():
            self.run_log[run_id] = set()

        self.run_log[run_id].add(text_to_log)
        return


    def save_log(self, output_path=None):
        output_path = output_path or self.log_file_path
        if len(self.run_log) == 0 and output_path:
            print(f"\nnothing to logg ...")
            return
        if len(self.run_log) > 0 and not output_path:
            print(f"\nlogg file path not defined - can't save the log with {len(self.run_log)} entries")
            return

        if output_path:
            # delete the file if already exists
            if os.path.isfile(output_path):
                os.remove(output_path)
            # create the folder if not exists to avoid log writing error on early crash
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            with open(output_path, "a") as f:
                for run_id, logs in self.run_log.items():
                    run = self.runs.get(run_id)
                    f.write(
                        f"#{run.id} - {czech_date(run.datetime)} - {run.locality.name} - {run.crop.name['en']} - {run.run_type.name['en']}\n")
                    for log in logs:
                        f.write(f"\t{log} \n")
                    f.write("\n")
            print(f"\nlog file saved to {output_path}")
        return


    def clear_log(self):
        self.run_log = {}
