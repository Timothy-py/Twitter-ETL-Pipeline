from crontab import CronTab

cron = CronTab(user=True)
cronjob = cron.new("consumer.py", "Run the consumer.py file app in this current directory")
cronjob.minute.every(1)

cron.write()
print("Job Done!")
