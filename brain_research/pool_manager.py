class PoolManager:
    def __init__(self):
        self.improve_pool = []
        self.submit_pool = []
        self.reject_pool = []
        self.archive_pool = []

    def route(self, alpha_id: str, decision: str):
        if decision == "improve_pool":
            self.improve_pool.append(alpha_id)
        elif decision == "submit_pool":
            self.submit_pool.append(alpha_id)
        elif decision == "reject_pool":
            self.reject_pool.append(alpha_id)
        else:
            self.archive_pool.append(alpha_id)
