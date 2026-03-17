import yaml
from pathlib import Path
from typing import Any, List, Dict


class Config:
    _instance = None
    _config: Dict = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def load(self, path: str = "config.yaml") -> None:
        config_path = Path(path)
        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")
        
        with open(config_path, 'r', encoding='utf-8') as f:
            self._config = yaml.safe_load(f)

    def get(self, key: str, default: Any = None) -> Any:
        keys = key.split('.')
        value = self._config
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            else:
                return default
            if value is None:
                return default
        return value

    @property
    def buffer_target(self) -> float:
        return self.get('buffer.target_minutes', 8)

    @property
    def buffer_min(self) -> float:
        return self.get('buffer.min_minutes', 3)

    @property
    def buffer_critical(self) -> float:
        return self.get('buffer.critical_seconds', 60)

    @property
    def talk_min_duration(self) -> int:
        return self.get('talk.min_duration_sec', 45)

    @property
    def talk_max_duration(self) -> int:
        return self.get('talk.max_duration_sec', 120)

    @property
    def talk_sparse_mode(self) -> bool:
        return self.get('talk.sparse_mode', False)

    @property
    def tts_sample_rate(self) -> int:
        return self.get('tts.sample_rate', 48000)

    @property
    def target_loudness(self) -> float:
        return self.get('audio.target_loudness_lufs', -16)

    @property
    def duck_db(self) -> float:
        return self.get('audio.duck_db', 10)

    @property
    def crossfade_sec(self) -> float:
        return self.get('audio.crossfade_sec', 2.0)

    @property
    def web_front_port(self) -> int:
        return self.get('server.web_front_port', 8000)

    @property
    def web_dashboard_port(self) -> int:
        return self.get('server.web_dashboard_port', 8001)

    @property
    def llm_config(self) -> Dict:
        return self.get('llm', {})

    @property
    def tts_config(self) -> Dict:
        return self.get('tts', {})

    @property
    def music_config(self) -> Dict:
        return self.get('music', {})

    @property
    def stream_config(self) -> Dict:
        return self.get('stream', {})

    @property
    def topics(self) -> List:
        return self.get('topics.rotation', [])

    @property
    def schedule(self) -> Dict:
        return self.get('schedule', {})

    @property
    def dj_intro_before_end_sec(self) -> int:
        return self.get('djs.intro_before_end_sec', 15)

    @property
    def dj_outro_duration_sec(self) -> int:
        return self.get('djs.outro_duration_sec', 7)

    @property
    def long_monologue_duration_sec(self) -> int:
        return self.get('djs.long_monologue_threshold_sec', 20)

    @property
    def djs_config(self) -> Dict:
        return self.get('djs', {})

    @property
    def current_program(self) -> Dict:
        from datetime import datetime
        return self.get_program_at(datetime.now())

    def get_program_at(self, dt) -> Dict:
        current_minutes = dt.weekday() * 24 * 60 + dt.hour * 60 + dt.minute
        return self._get_program_at_minutes(current_minutes)

    def get_next_program(self, dt=None) -> Dict:
        from datetime import datetime, timedelta
        if dt is None:
            dt = datetime.now()
        
        # Look ahead, but skip the current one
        current_minutes = dt.weekday() * 24 * 60 + dt.hour * 60 + dt.minute
        # Search for the first program that starts AFTER the current one
        # To be safe, we check every 30 mins for the next 24 hours
        for i in range(1, 48):
            next_dt = dt + timedelta(minutes=i*30)
            next_prog = self.get_program_at(next_dt)
            if next_prog and next_prog.get('title') != self.get_program_at(dt).get('title'):
                return next_prog
        return self.get_program_at(dt)

    def _get_program_at_minutes(self, minutes: int) -> dict:
        programs = self.get('schedule.programs', [])
        if not programs:
            return {}
            
        day_map = {
            "mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6,
            "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3, "friday": 4, "saturday": 5, "sunday": 6,
            "weekdays": [0, 1, 2, 3, 4], "weekends": [5, 6], "everyday": [0, 1, 2, 3, 4, 5, 6],
            "all": [0, 1, 2, 3, 4, 5, 6]
        }
        
        parsed_programs = []
        for p in programs:
            try:
                time_parts = str(p.get('start_time', '00:00')).split(':')
                minutes_in_day = int(time_parts[0]) * 60 + int(time_parts[1])
                
                days = p.get('days', ['everyday'])
                if isinstance(days, str): days = [days]
                    
                target_days = set()
                for d in days:
                    d_lower = str(d).lower()
                    if d_lower in day_map:
                        val = day_map[d_lower]
                        if isinstance(val, list): target_days.update(val)
                        else: target_days.add(val)
                if not target_days: target_days = set(range(7))
                    
                for d in target_days:
                    abs_minutes = d * 24 * 60 + minutes_in_day
                    parsed_programs.append((abs_minutes, p))
                    parsed_programs.append((abs_minutes - 7 * 24 * 60, p))
                    parsed_programs.append((abs_minutes + 7 * 24 * 60, p))
            except Exception: pass
                
        parsed_programs.sort(key=lambda x: x[0])
        if not parsed_programs: return programs[0]
            
        active = parsed_programs[0][1]
        for abs_minutes, p in parsed_programs:
            if abs_minutes <= minutes:
                active = p
            else:
                break
        return active


config = Config()
