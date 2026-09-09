// Pegar en la consola del navegador con /dashboard abierto. Usa datos temporales;
// no envía solicitudes ni modifica las opciones del formulario real.
(() => {
  const assert = (condition, message) => {
    if (!condition) throw new Error(message);
  };
  const form = document.createElement("form");
  form.innerHTML = `
    <label for="test_responsable_select">Responsable
      <div hidden>
        <input id="test_responsable_input" role="combobox" aria-expanded="false"
          aria-controls="test_responsable_dropdown">
        <div id="test_responsable_dropdown" role="listbox" hidden></div>
      </div>
      <select id="test_responsable_select" name="responsable_usuario_id" required>
        <option value="7" data-auditor-id="42">José Ángel Pérez (Resguardante)</option>
        <option value="auditor:42">José Ángel Pérez</option>
        <option value="auditor:43" selected>María Núñez</option>
        <option value="auditor:44" disabled>No disponible</option>
        <option value="auditor:45" hidden>Oculto</option>
      </select>
    </label>`;
  const previousFocus = document.activeElement;
  const previousScroll = { left: window.scrollX, top: window.scrollY };
  document.body.appendChild(form);
  try {
    const input = form.querySelector("input");
    const select = form.querySelector("select");
    const dropdown = form.querySelector('[role="listbox"]');
    const combo = setupCombobox(input.id, dropdown.id, select.id);
    const items = () => [...dropdown.querySelectorAll("[data-value]")];
    const search = (value) => {
      input.value = value;
      input.dispatchEvent(new Event("input", { bubbles: true }));
    };
    const key = (value) => input.dispatchEvent(new KeyboardEvent("keydown", { key: value, bubbles: true, cancelable: true }));
    let changes = 0;
    form.addEventListener("change", () => changes++);

    assert(input.value === "María Núñez", "Debe mostrar la selección inicial del servidor");
    assert(select.hidden && !input.parentElement.hidden, "Debe activar el campo visible con JS");
    assert(form.querySelector("label").control === input, "La etiqueta debe enfocar el campo visible");
    assert(input.required && select.required, "Debe conservar la obligatoriedad");
    input.focus();
    assert(items().length === 3, "Debe listar también la selección actual, sin opciones restringidas");
    assert(input.getAttribute("aria-expanded") === "true", "Debe anunciar la lista abierta");
    for (const term of ["jose", "JOSÉ", "érez", "ANGEL"]) {
      search(term);
      assert(items().length === 2, `Búsqueda parcial sin distinguir acentos o mayúsculas: ${term}`);
    }
    items()[1].click();
    assert(select.value === "auditor:42" && input.value === "José Ángel Pérez", "Debe seleccionar un auditor");
    assert(new FormData(form).get("responsable_usuario_id") === "auditor:42", "Debe enviar la referencia del auditor");
    assert(dropdown.hidden && changes === 1, "Debe cerrar la lista y emitir change con propagación");
    input.click();
    assert(items().length === 3, "Debe reabrir todas las opciones después de seleccionar");
    search("jose");
    key("ArrowDown");
    key("ArrowDown");
    assert(document.getElementById(input.getAttribute("aria-activedescendant")) === items()[1], "Debe anunciar la opción recorrida");
    key("ArrowUp");
    key("Enter");
    assert(select.value === "7" && changes === 2, "Debe seleccionar el resguardante con teclado");
    assert(select.selectedOptions[0].dataset.auditorId === "42", "Debe conservar data-auditor-id");
    assert(new FormData(form).getAll("responsable_usuario_id").join() === "7", "Debe enviar un único valor numérico");

    const selectedName = input.value;
    search("Nombre inexistente");
    assert(items().length === 0 && dropdown.textContent === "No se encontraron resultados", "Debe mostrar ausencia de resultados");
    key("Enter");
    assert(select.value === "7" && changes === 2, "Escribir no debe crear ni seleccionar nombres");
    key("Escape");
    assert(dropdown.hidden && input.value === selectedName, "Escape debe restaurar la selección");
    assert(input.getAttribute("aria-expanded") === "false" && !input.hasAttribute("aria-activedescendant"), "Debe limpiar el estado accesible al cerrar");
    search("Busqueda cancelada");
    input.blur();
    assert(dropdown.hidden && input.value === selectedName, "Salir del campo debe cancelar la búsqueda");

    select.innerHTML = '<option value="" disabled selected>No hay personal registrado</option>';
    combo.refreshOptions();
    select.dispatchEvent(new Event("change"));
    input.focus();
    assert(items().length === 0 && dropdown.textContent === "No se encontraron resultados", "Debe admitir un catálogo vacío");
    search("Texto libre");
    assert(!form.reportValidity() && document.activeElement === input, "La obligatoriedad debe impedir texto libre y enfocar el campo visible");
    assert(input.getAttribute("aria-invalid") === "true", "Debe anunciar el error de validación");
    console.log("OK: búsqueda, identidades, envío, change, teclado, cancelación y catálogo vacío.");
  } finally {
    form.remove();
    previousFocus?.focus({ preventScroll: true });
    window.scrollTo(previousScroll);
  }
})();
