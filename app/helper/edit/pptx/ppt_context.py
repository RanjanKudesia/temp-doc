"""PPT slide membership tracking context."""

from typing import Any


class _PptContext:
    """Tracks which slide each paragraph/table/media dict belongs to.

    Uses Python object identity (id()) so the mapping survives index
    renumbering that occurs during normalisation.
    """

    def __init__(self, document: dict[str, Any]) -> None:
        self.para_id_to_slide: dict[int, int] = {}
        self.table_id_to_slide: dict[int, int] = {}
        self.media_id_to_slide: dict[int, int] = {}
        self._init(document)

    # ------------------------------------------------------------------
    def _init(self, document: dict[str, Any]) -> None:
        slides = document.get("slides") or []
        paragraphs = document.get("paragraphs") or []
        tables = document.get("tables") or []
        media = document.get("media") or []

        pi_to_slide: dict[int, int] = {}
        ti_to_slide: dict[int, int] = {}
        mi_to_slide: dict[int, int] = {}

        # Build index->slide mappings from slide metadata
        for slide in slides:
            if not isinstance(slide, dict):
                continue
            si = slide.get("index")
            if not isinstance(si, int):
                continue
            self._map_slide_items(slide, pi_to_slide, ti_to_slide, mi_to_slide)

        # Map object identity to slide index
        self._map_paragraphs(paragraphs, pi_to_slide)
        self._map_tables(tables, ti_to_slide)
        self._map_media(media, mi_to_slide)

    def _map_slide_items(
        self,
        slide: dict[str, Any],
        pi_to_slide: dict[int, int],
        ti_to_slide: dict[int, int],
        mi_to_slide: dict[int, int],
    ) -> None:
        """Map item indices within a slide to slide index."""
        si = slide.get("index")
        if not isinstance(si, int):
            return

        for pi in (slide.get("paragraph_indices") or []):
            if isinstance(pi, int):
                pi_to_slide[pi] = si
        for ti in (slide.get("table_indices") or []):
            if isinstance(ti, int):
                ti_to_slide[ti] = si
        for mi in (slide.get("media_indices") or []):
            if isinstance(mi, int):
                mi_to_slide[mi] = si

    def _map_paragraphs(
        self, paragraphs: list[Any], pi_to_slide: dict[int, int]
    ) -> None:
        """Map paragraph object ids to slide indices."""
        for p in paragraphs:
            if isinstance(p, dict):
                si = pi_to_slide.get(p.get("index"))  # type: ignore[arg-type]
                if si is not None:
                    self.para_id_to_slide[id(p)] = si

    def _map_tables(
        self, tables: list[Any], ti_to_slide: dict[int, int]
    ) -> None:
        """Map table object ids to slide indices."""
        for t in tables:
            if isinstance(t, dict):
                si = ti_to_slide.get(t.get("index"))  # type: ignore[arg-type]
                if si is not None:
                    self.table_id_to_slide[id(t)] = si

    def _map_media(
        self, media: list[Any], mi_to_slide: dict[int, int]
    ) -> None:
        """Map media object ids to slide indices."""
        for m in media:
            if isinstance(m, dict):
                si = mi_to_slide.get(m.get("index"))  # type: ignore[arg-type]
                if si is not None:
                    self.media_id_to_slide[id(m)] = si

    # ------------------------------------------------------------------
    def slide_for_para_index(
        self, document: dict[str, Any], para_index: int
    ) -> int | None:
        """Return slide_index for the paragraph that currently has para_index."""
        for p in (document.get("paragraphs") or []):
            if isinstance(p, dict) and p.get("index") == para_index:
                return self.para_id_to_slide.get(id(p))
        return None

    def slide_for_table_index(
        self, document: dict[str, Any], table_index: int
    ) -> int | None:
        """Return slide_index for the table that currently has table_index."""
        for t in (document.get("tables") or []):
            if isinstance(t, dict) and t.get("index") == table_index:
                return self.table_id_to_slide.get(id(t))
        return None

    def assign_new_paragraphs(
        self,
        document: dict[str, Any],
        known_ids: set[int],
        slide_index: int | None,
    ) -> None:
        """Assign any paragraph dicts not in known_ids to slide_index."""
        if slide_index is None:
            return
        for p in (document.get("paragraphs") or []):
            if isinstance(p, dict) and id(p) not in known_ids:
                self.para_id_to_slide[id(p)] = slide_index

    def assign_new_tables(
        self,
        document: dict[str, Any],
        known_ids: set[int],
        slide_index: int | None,
    ) -> None:
        """Assign any table dicts not in known_ids to slide_index."""
        if slide_index is None:
            return
        for t in (document.get("tables") or []):
            if isinstance(t, dict) and id(t) not in known_ids:
                self.table_id_to_slide[id(t)] = slide_index

    def rebuild_slide_indices(self, document: dict[str, Any]) -> None:
        """Rebuild per-slide index arrays from current flat collections."""
        slides = document.get("slides") or []
        paragraphs = document.get("paragraphs") or []
        tables = document.get("tables") or []
        media = document.get("media") or []

        slide_paras: dict[int, list[int]] = {}
        slide_tables: dict[int, list[int]] = {}
        slide_medias: dict[int, list[int]] = {}

        # Initialize per-slide lists
        for slide in slides:
            if isinstance(slide, dict):
                si = slide.get("index")
                if isinstance(si, int):
                    slide_paras[si] = []
                    slide_tables[si] = []
                    slide_medias[si] = []

        # Map items to slides
        self._populate_slide_items(paragraphs, slide_paras, "para")
        self._populate_slide_items(tables, slide_tables, "table")
        self._populate_slide_items(media, slide_medias, "media")

        # Update slide metadata
        for slide in slides:
            if not isinstance(slide, dict):
                continue
            si = slide.get("index")
            if not isinstance(si, int):
                continue
            self._update_slide_metadata(slide, slide_paras.get(si, []),
                                        slide_tables.get(si, []),
                                        slide_medias.get(si, []))

    def _populate_slide_items(
        self,
        items: list[Any],
        slide_dict: dict[int, list[int]],
        item_type: str,
    ) -> None:
        """Populate slide_dict with item indices by slide."""
        mapping = {
            "para": self.para_id_to_slide,
            "table": self.table_id_to_slide,
            "media": self.media_id_to_slide,
        }
        id_map = mapping.get(item_type)
        if not id_map:
            return

        for item in items:
            if isinstance(item, dict):
                sid = id_map.get(id(item))
                if sid is not None and sid in slide_dict:
                    slide_dict[sid].append(item.get("index", 0))

    def _update_slide_metadata(
        self,
        slide: dict[str, Any],
        para_indices: list[int],
        table_indices: list[int],
        media_indices: list[int],
    ) -> None:
        """Update slide metadata fields with counts and indices."""
        slide["paragraph_indices"] = sorted(para_indices)
        slide["table_indices"] = sorted(table_indices)
        slide["media_indices"] = sorted(media_indices)
        slide["shape_count"] = len(para_indices) + \
            len(table_indices) + len(media_indices)
        slide["table_count"] = len(table_indices)
        slide["image_count"] = len(media_indices)
