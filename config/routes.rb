Rails.application.routes.draw do
  get '', to: redirect('/menu/index')
  get 'menu/index'
  get 'query/test'
  get 'query/large_object'
  # For details on the DSL available within this file, see https://guides.rubyonrails.org/routing.html
end
