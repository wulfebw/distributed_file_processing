class StrategyFactory(object):
	"""
	:description: in python, a factory can be a dictionary that maps identifiers to either classes or functions. I'm making this a class in order to keep the error handling out of the main function.
	"""

	def __init__(self):
		self.strategies = dict()

	def register(self, id, strategy):
		"""
		:description: registers a strategy with the factory - overwrites previous strategies with the same id and ids can be anything

		:type id: anything?
		:param id: the id used to retrieve a given strategy

		:type strategy: a function
		:param strategy: the function to call as the strategy
		"""
		self.strategies[id] = strategy

	def get_strategy(self, id):
		try:
			return self.strategies[id]
		except KeyError as e:
			raise KeyError("the provided strategy id does not exist or is not registered")